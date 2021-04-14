#ifndef HERMES_ARROW_HH
#define HERMES_ARROW_HH

#include <functional>
#include <memory>

namespace arrow {
class Buffer;
class RecordBatch;
class Schema;
class Scalar;
class Table;
}  // namespace arrow

namespace hermes {

// helper functions to deal with arrow
std::shared_ptr<arrow::Buffer> serialize(const std::shared_ptr<arrow::RecordBatch> &batch,
                                         const std::shared_ptr<arrow::Schema> &schema);
std::shared_ptr<arrow::Table> deserialize(const std::shared_ptr<arrow::Buffer> &buffer);

std::shared_ptr<arrow::RecordBatch> get_batch(const std::shared_ptr<arrow::Buffer> &buffer);

std::shared_ptr<arrow::Table> load_table(const std::string &filename);

uint8_t get_uint8(const std::shared_ptr<arrow::Scalar> &scalar);
uint16_t get_uint16(const std::shared_ptr<arrow::Scalar> &scalar);
uint32_t get_uint32(const std::shared_ptr<arrow::Scalar> &scalar);
uint64_t get_uint64(const std::shared_ptr<arrow::Scalar> &scalar);
std::string get_string(const std::shared_ptr<arrow::Scalar> &scalar);
bool get_bool(const std::shared_ptr<arrow::Scalar> &scalar);

std::vector<uint64_t> get_uint64s(const std::shared_ptr<arrow::Scalar> &scalar);
std::vector<bool> get_bools(const std::shared_ptr<arrow::Scalar> &scalar);

}  // namespace hermes

#endif  // HERMES_ARROW_HH
