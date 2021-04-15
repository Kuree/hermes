#ifndef HERMES_SERIALIZER_HH
#define HERMES_SERIALIZER_HH

#include <mutex>
#include <unordered_set>

#include "event.hh"
#include "transaction.hh"

namespace parquet {
class WriterProperties;
namespace arrow {
class FileWriter;
}
}  // namespace parquet

namespace hermes {

struct SerializationStat {
    std::string parquet_filename;
    std::string json_filename;

    std::string type;

    // could be empty
    std::string name;
};

class Serializer {
public:
    explicit Serializer(std::string output_dir);
    Serializer(std::string output_dir, bool override);

    bool serialize(EventBatch &batch);
    bool serialize(TransactionBatch &batch);
    bool serialize(TransactionGroupBatch &batch);

    void finalize();

    ~Serializer() { finalize(); }

private:
    std::string output_dir_;
    uint64_t batch_counter_ = 0;
    std::mutex batch_mutex_;
    std::shared_ptr<parquet::WriterProperties> writer_properties_;
    std::unordered_map<const void *, std::shared_ptr<parquet::arrow::FileWriter>> writers_;
    std::unordered_map<const void *, SerializationStat> stats_;

    std::pair<std::string, std::string> get_next_filename();
    parquet::arrow::FileWriter *get_writer(const void *ptr,
                                           const std::shared_ptr<arrow::Schema> &schema);
    SerializationStat &get_stat(const void *ptr);
    void identify_batch_counter();

    static bool serialize(parquet::arrow::FileWriter *writer,
                   const std::shared_ptr<arrow::RecordBatch> &record);
    static void update_stat(SerializationStat &stat, const EventBatch &batch);
    static void update_stat(SerializationStat &stat, const TransactionBatch &batch);
    static void update_stat(SerializationStat &stat, const TransactionGroupBatch &batch);
    static void write_stat(const SerializationStat &stat);
};

}  // namespace hermes

#endif  // HERMES_SERIALIZER_HH
