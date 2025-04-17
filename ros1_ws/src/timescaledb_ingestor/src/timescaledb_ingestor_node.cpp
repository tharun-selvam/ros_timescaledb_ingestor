#include <ros/ros.h>
#include <ros/spinner.h> // Using AsyncSpinner
#include <sensor_msgs/LaserScan.h>
#include <pqxx/pqxx> // Use the directory name 'pqxx'
#include <nlohmann/json.hpp> // nlohmann json headers

#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip> // For std::put_time
#include <chrono>  // For timing
#include <atomic>  // For stop flag

// Use nlohmann json alias
using json = nlohmann::json;

// --- Thread-Safe Queue Implementation ---
// Simple blocking queue for batches (std::vector<std::vector<std::string>>)
template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cond_;
    size_t maxSize_; // Max size of the queue

public:
    ThreadSafeQueue(size_t maxSize = 10) : maxSize_(maxSize) {}

    bool tryPush(T item, const std::chrono::milliseconds& timeout = std::chrono::milliseconds(500)) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cond_.wait_for(lock, timeout, [this]{ return queue_.size() < maxSize_; })) {
            // Timed out waiting for space or queue still full
            return false; // Indicate failure (queue full)
        }
        queue_.push(std::move(item));
        lock.unlock(); // Unlock before notifying
        cond_.notify_one();
        return true; // Indicate success
    }

    bool pop(T& item, const std::chrono::milliseconds& timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cond_.wait_for(lock, timeout, [this]{ return !queue_.empty(); })) {
            return false; // Indicate timeout (queue empty)
        }
        item = std::move(queue_.front());
        queue_.pop();
        return true; // Indicate success
    }

    size_t size() {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.size();
    }

    bool empty() {
        std::unique_lock<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    // Notify potentially waiting pop() calls (e.g., during shutdown)
    void notifyAll() {
        cond_.notify_all();
    }
};


// --- Ingestor Class ---
class TimescaleDBIngestor {
private:
    // ROS members
    ros::NodeHandle nh_;
    ros::NodeHandle pnh_; // Private Node Handle for parameters
    ros::Subscriber sub_;
    ros::Timer timer_;

    // Parameters
    std::string topic_name_;
    std::string db_table_name_;
    std::string db_conn_str_; // Connection string for pqxx
    int batch_size_;
    double flush_interval_sec_;

    // Buffering & Threading
    std::vector<std::vector<std::string>> current_batch_; // Buffer in message callback
    std::mutex batch_mutex_; // Mutex for current_batch_
    ThreadSafeQueue<std::vector<std::vector<std::string>>> db_write_queue_; // Queue for writer thread
    std::thread db_worker_thread_;
    std::atomic<bool> stop_worker_flag_{false}; // Atomic flag to signal worker stop

    // Statistics
    std::atomic<long long> total_inserted_count_{0};

    // --- Database Worker ---
    void dbWorkerLoop() {
        ROS_INFO("DB Writer thread started.");
        std::unique_ptr<pqxx::connection> C; // Use unique_ptr for connection management

        auto connect_db = [&]() {
            if (C && C->is_open()) return true; // Already connected
            try {
                ROS_INFO("DB Writer: Connecting to database...");
                C = std::make_unique<pqxx::connection>(db_conn_str_);
                if (C->is_open()) {
                    ROS_INFO("DB Writer: Database connection successful.");
                    return true;
                } else {
                    ROS_ERROR("DB Writer: Database connection failed (unknown reason).");
                    C.reset(); // Ensure connection is null if failed
                    return false;
                }
            } catch (const std::exception &e) {
                ROS_ERROR("DB Writer: Database connection failed: %s. Retrying in 5 seconds...", e.what());
                C.reset();
                ros::Duration(5.0).sleep(); // Simple retry delay
                return false;
            }
        };

        while (!stop_worker_flag_.load()) {
            std::vector<std::vector<std::string>> batch_to_write;

            // Wait for data on the queue
            if (!db_write_queue_.pop(batch_to_write, std::chrono::milliseconds(1000))) {
                // Timeout occurred, check stop flag and loop again
                continue;
            }

            if (batch_to_write.empty()) continue; // Should not happen, but safety check

            // Ensure connection
            if (!connect_db()) {
                ROS_ERROR("DB Writer: Reconnect failed. Dropping batch.");
                continue;
            }

                    // --- Use COPY FROM for high performance ---
            try {
                pqxx::work W(*C); // Start transaction

                // Define columns in the COPY command - MUST match order in row_data
                std::string copy_sql = db_table_name_ +
                                    " (time, frame_id, angle_min, angle_max, angle_increment, "
                                    "time_increment, scan_time, range_min, range_max, ranges, intensities) ";

                // --- Timing Start ---
                auto start = std::chrono::steady_clock::now();

                // Create the stream_to object
                // Create the stream_to object
                pqxx::stream_to stream(W, copy_sql);

                // Process each row in the batch
                for (const auto& row : batch_to_write) {
                    // pqxx::stream_to works with tuples row by row
                    // We need to build a tuple dynamically based on the number of columns
                    
                    // For a fixed set of columns (11 in your case), you can do:
                    if (row.size() == 11) {
                        stream << std::make_tuple(
                            row[0],  // time
                            row[1],  // frame_id
                            row[2],  // angle_min
                            row[3],  // angle_max
                            row[4],  // angle_increment
                            row[5],  // time_increment
                            row[6],  // scan_time
                            row[7],  // range_min
                            row[8],  // range_max
                            row[9],  // ranges
                            row[10]  // intensities
                        );
                    } else {
                        // Handle error - unexpected number of columns
                        ROS_ERROR("Unexpected number of columns in row: %zu (expected 11)", row.size());
                    }
                }

                // Complete the COPY operation
                stream.complete();
                // --- End Correction ---

                W.commit(); // Commit transaction

                // --- Timing End ---
                auto end = std::chrono::steady_clock::now();
                std::chrono::duration<double> duration = end - start;

                long long batch_count = batch_to_write.size();
                total_inserted_count_ += batch_count;
                size_t q_size = db_write_queue_.size();

                ROS_INFO("DB Writer: Batch committed via COPY (%lld rows, took %.4fs). Total: %lld. Queue size: %zu",
                        batch_count, duration.count(), total_inserted_count_.load(), q_size);

            } catch (const std::exception &e) {
                ROS_ERROR("DB Writer: Database COPY/commit error: %s", e.what());
                // ... (error handling) ...
                try {
                    if (C && C->is_open()) {
                    pqxx::work W(*C); W.abort();
                    }
                } catch (const std::exception &e_abort) {
                    ROS_ERROR("DB Writer: Rollback failed: %s", e_abort.what());
                }
                C.reset();
            }
        }

        ROS_INFO("DB Writer thread finishing.");
         if (C && C->is_open()) C->disconnect(); // Close connection
    }

    // --- Called by timer or when batch is full ---
    void flushBuffer() {
        std::vector<std::vector<std::string>> batch_copy;
        { // Lock scope
            std::lock_guard<std::mutex> lock(batch_mutex_);
            if (current_batch_.empty()) {
                return;
            }
            // Swap is efficient way to move data and clear original
            batch_copy.swap(current_batch_);
        } // Mutex released

        if (!batch_copy.empty()) {
            if (!db_write_queue_.tryPush(std::move(batch_copy))) {
                 ROS_WARN("DB write queue is full! Dropping batch of %zu.", batch_copy.size());
            } else {
                 ROS_DEBUG("Placed batch of %zu onto DB write queue (current qsize: %zu).", batch_copy.size(), db_write_queue_.size());
            }
        }
    }

    // --- ROS Callbacks ---
    void timerCallback(const ros::TimerEvent& event) {
        flushBuffer();
    }

    void messageCallback(const sensor_msgs::LaserScan::ConstPtr& msg) {
         // This vector holds strings, prepared for COPY format
        std::vector<std::string> row_data;
        row_data.reserve(11); // Reserve space for known number of columns

        bool valid_row = true;

        // 1. Time (header.stamp)
        std::string timestamp_str;
        if (msg->header.stamp.isZero()) {
             ROS_WARN_THROTTLE(5, "Timestamp field 'header.stamp' is zero. Skipping message.");
             valid_row = false;
        } else {
             // Format to ISO 8601 style suitable for TIMESTAMPTZ
             double secs = msg->header.stamp.toSec();
             time_t tv_sec = static_cast<time_t>(secs);
             // Calculate fractional seconds (microseconds)
             long long nanosec_part = static_cast<long long>((secs - tv_sec) * 1e9);
             long long usec_part = nanosec_part / 1000; // Convert ns to us

             std::stringstream ss;
             // Use std::put_time to format the time part, ensuring UTC ('Z')
             ss << std::put_time(std::gmtime(&tv_sec), "%Y-%m-%d %H:%M:%S");
             // Append microseconds and timezone indicator
             ss << "." << std::setfill('0') << std::setw(6) << usec_part << "+00";
             timestamp_str = ss.str();
        }
        row_data.push_back(timestamp_str);

        // 2. Frame ID (header.frame_id) - Check validity early
        if (!valid_row) return; // Don't process further if timestamp was invalid
        row_data.push_back(msg->header.frame_id);

        // 3. Scalar Floats
        row_data.push_back(std::to_string(msg->angle_min));
        row_data.push_back(std::to_string(msg->angle_max));
        row_data.push_back(std::to_string(msg->angle_increment));
        row_data.push_back(std::to_string(msg->time_increment));
        row_data.push_back(std::to_string(msg->scan_time));
        row_data.push_back(std::to_string(msg->range_min));
        row_data.push_back(std::to_string(msg->range_max));

        // 4. Ranges (vector<float> to JSON string)
        try {
             json ranges_json = msg->ranges; // nlohmann/json converts vector directly
             row_data.push_back(ranges_json.dump());
        } catch (const std::exception& e) {
             ROS_WARN("JSON conversion error for ranges: %s. Storing empty array.", e.what());
             row_data.push_back("[]");
        }

        // 5. Intensities (vector<float> to JSON string)
         try {
             json intensities_json = msg->intensities;
             row_data.push_back(intensities_json.dump());
        } catch (const std::exception& e) {
             ROS_WARN("JSON conversion error for intensities: %s. Storing empty array.", e.what());
             row_data.push_back("[]");
        }

        // Add the fully prepared row (vector of strings) to the local batch
        { // Lock scope
             std::lock_guard<std::mutex> lock(batch_mutex_);
             current_batch_.push_back(std::move(row_data));
             // Flush immediately if batch size reached
             if (current_batch_.size() >= batch_size_) {
                 flushBuffer();
             }
        } // Mutex released
    }


public:
    TimescaleDBIngestor() : nh_(), pnh_("~"), db_write_queue_(10) { // Initialize queue size
        // Get parameters
        pnh_.param<std::string>("topic_name", topic_name_, "/scan"); // Default topic
        pnh_.param<std::string>("db_table_name", db_table_name_, "laser_scans"); // Default table
        pnh_.param<int>("batch_size", batch_size_, 100);
        pnh_.param<double>("flush_interval", flush_interval_sec_, 5.0);

        // Construct connection string from individual params
        std::string db_host, db_name, db_user, db_pass;
        int db_port;
        if (!pnh_.getParam("db_host", db_host)) ROS_FATAL("Required parameter 'db_host' missing!");
        if (!pnh_.getParam("db_name", db_name)) ROS_FATAL("Required parameter 'db_name' missing!");
        if (!pnh_.getParam("db_user", db_user)) ROS_FATAL("Required parameter 'db_user' missing!");
        if (!pnh_.getParam("db_password", db_pass)) ROS_FATAL("Required parameter 'db_password' missing!");
        pnh_.param<int>("db_port", db_port, 5432);

        // Basic connection string format for pqxx
        db_conn_str_ = "dbname=" + db_name + " user=" + db_user + " password=" + db_pass +
                       " hostaddr=" + db_host + " port=" + std::to_string(db_port);
        ROS_INFO("DB Connection String (Password Hidden): dbname=%s user=%s hostaddr=%s port=%d",
                 db_name.c_str(), db_user.c_str(), db_host.c_str(), db_port);


        // Start worker thread
        stop_worker_flag_.store(false);
        db_worker_thread_ = std::thread(&TimescaleDBIngestor::dbWorkerLoop, this);

        // Setup Subscriber and Timer
        sub_ = nh_.subscribe(topic_name_, batch_size_ * 2, &TimescaleDBIngestor::messageCallback, this);
        timer_ = nh_.createTimer(ros::Duration(flush_interval_sec_), &TimescaleDBIngestor::timerCallback, this);

        ROS_INFO("TimescaleDB C++ Ingestor node started for topic '%s'.", topic_name_.c_str());
        ROS_INFO("Ingesting to table '%s'. Batch size: %d, Flush interval: %.2fs.",
                 db_table_name_.c_str(), batch_size_, flush_interval_sec_);
    }

    ~TimescaleDBIngestor() {
        ROS_INFO("Ingestor node shutting down...");
        stop_worker_flag_.store(true);
        db_write_queue_.notifyAll(); // Wake up worker if it's waiting on pop
        if (db_worker_thread_.joinable()) {
             ROS_INFO("Waiting for DB writer thread to join...");
             db_worker_thread_.join(); // Wait for worker to finish
        }
        ROS_INFO("Shutdown complete. Final total inserted count: %lld", total_inserted_count_.load());
    }
}; // End class TimescaleDBIngestor


int main(int argc, char **argv) {
    ros::init(argc, argv, "timescaledb_ingestor_cpp_node");

    TimescaleDBIngestor ingestor_node;

    // Use AsyncSpinner to handle callbacks in separate threads
    // Number of threads = 0 means use number of hardware cores
    ros::AsyncSpinner spinner(0);
    spinner.start();

    ros::waitForShutdown(); // Wait until ROS shutdown signal

    return 0;
}