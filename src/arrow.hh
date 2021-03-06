#ifndef HERMES_ARROW_HH
#define HERMES_ARROW_HH

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>

namespace arrow {
class Buffer;
class RecordBatch;
class Schema;
class Scalar;
class Table;
class Field;
class Array;

namespace fs {
class FileSystem;
}
}  // namespace arrow

namespace hermes {

class EventBatch;
class TransactionBatch;

// helper functions to deal with arrow
std::shared_ptr<arrow::Buffer> serialize(const std::shared_ptr<arrow::RecordBatch> &batch,
                                         const std::shared_ptr<arrow::Schema> &schema);

void serialize(const EventBatch *batch, std::vector<std::shared_ptr<arrow::Field>> &fields,
               std::vector<std::shared_ptr<arrow::Array>> &arrays);
void serialize(const TransactionBatch *batch, std::vector<std::shared_ptr<arrow::Field>> &fields,
               std::vector<std::shared_ptr<arrow::Array>> &arrays);

std::shared_ptr<arrow::Table> deserialize(const std::shared_ptr<arrow::Buffer> &buffer);
bool deserialize(EventBatch *batch, const arrow::Table *table,
                 const std::unordered_set<std::string> &fields);
bool deserialize(TransactionBatch *batch, const arrow::Table *table,
                 const std::unordered_set<std::string> &fields);

std::shared_ptr<arrow::RecordBatch> get_batch(const std::shared_ptr<arrow::Buffer> &buffer);

std::shared_ptr<arrow::Table> load_table(const std::string &filename);

uint8_t get_uint8(const std::shared_ptr<arrow::Scalar> &scalar);
uint16_t get_uint16(const std::shared_ptr<arrow::Scalar> &scalar);
int16_t get_int16(const std::shared_ptr<arrow::Scalar> &scalar);
uint32_t get_uint32(const std::shared_ptr<arrow::Scalar> &scalar);
int32_t get_int32(const std::shared_ptr<arrow::Scalar> &scalar);
uint64_t get_uint64(const std::shared_ptr<arrow::Scalar> &scalar);
int64_t get_int64(const std::shared_ptr<arrow::Scalar> &scalar);
std::string get_string(const std::shared_ptr<arrow::Scalar> &scalar);
bool get_bool(const std::shared_ptr<arrow::Scalar> &scalar);

std::vector<uint64_t> get_uint64s(const std::shared_ptr<arrow::Scalar> &scalar);
std::vector<bool> get_bools(const std::shared_ptr<arrow::Scalar> &scalar);

struct FileSystemInfo {
public:
    explicit FileSystemInfo(const std::string &path);
    std::string path;
    std::string access_key;
    std::string secret_key;
    std::string end_point;
    [[nodiscard]] bool is_s3() const { return is_s3_; }

    // clear the directory
    void clear() const;

private:
    bool is_s3_ = false;
};

std::shared_ptr<arrow::fs::FileSystem> load_fs(const FileSystemInfo &info);

}  // namespace hermes

#endif  // HERMES_ARROW_HH
