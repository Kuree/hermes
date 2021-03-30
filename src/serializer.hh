#ifndef HERMES_SERIALIZER_HH
#define HERMES_SERIALIZER_HH

#include <mutex>

#include "event.hh"
#include "transaction.hh"

namespace parquet {
class WriterProperties;
}

namespace hermes {

class Serializer {
public:
    explicit Serializer(std::string output_dir);

    bool serialize(const EventBatch &batch);
    bool serialize(const TransactionBatch &batch);

private:
    std::string output_dir_;
    uint64_t batch_counter_ = 0;
    std::mutex batch_mutex_;
    std::shared_ptr<parquet::WriterProperties> writer_properties_;

    std::pair<std::string, std::string> get_next_filename();
    bool serialize(const std::string &filename, const std::shared_ptr<arrow::RecordBatch> &batch);
    static void write_stat(const std::string &json_filename, const std::string &parquet_filename,
                           const EventBatch &batch);
    static void write_stat(const std::string &json_filename, const std::string &parquet_filename,
                           const TransactionBatch &batch);
};
}  // namespace hermes

#endif  // HERMES_SERIALIZER_HH
