#include <mpi.h>
#include <iostream>

#include <arrow/api.h>
#include <arrow/array/builder_primitive.h>
#include <glog/logging.h>

#include "all_to_all.hpp"
#include "arrow/arrow_all_to_all.hpp"

using arrow::DoubleBuilder;
using arrow::Int64Builder;

class Clbk : public twisterx::ArrowCallback {
public:
  bool onReceive(int source, std::shared_ptr<arrow::Table> table) override {
    LOG(INFO) << "Received 000000000000000000000000000000000000000";

    auto ids =
        std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    auto costs =
        std::static_pointer_cast<arrow::DoubleArray>(table->column(1)->chunk(0));


    for (int64_t i = 0; i < table->num_rows(); i++) {
      // Another simplification in this example is that we assume that there are
      // no null entries, e.g. each row is fill with valid values.
      int64_t id = ids->Value(i);
      double cost = costs->Value(i);

      LOG(INFO) << "ID " << id << " cost " << cost;
    }
    return true;
  }
};

int main(int argc, char *argv[]) {
  std::cout << "First - ";
  MPI_Init(NULL, NULL);
  int rank = 0;
  int size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  std::cout << "First - " << rank << " size " << size << std::endl;
  std::vector<int> sources;
  std::vector<int> targets;
  for (int i = 0; i < size; i++) {
    sources.push_back(i);
    targets.push_back(i);
  }

  arrow::MemoryPool* pool = arrow::default_memory_pool();

  Int64Builder id_builder(pool);
  DoubleBuilder cost_builder(pool);

  for (int i = 0; i < 10; i++) {
    id_builder.Append(10 + i);
    cost_builder.Append(0.2 + i);
  }

  Clbk clbk;

  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::int64()), arrow::field("cost", arrow::float64())};
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  twisterx::ArrowAllToAll all(rank, sources, targets, 0, &clbk, schema);

  std::shared_ptr<arrow::Array> id_array;
  id_builder.Finish(&id_array);
  std::shared_ptr<arrow::Array> cost_array;
  cost_builder.Finish(&cost_array);

  std::shared_ptr<arrow::Table> ptr = arrow::Table::Make(schema, {id_array, cost_array});
  all.insert(ptr, rank);

  all.finish();
  while (!all.isComplete()) {
  }
  all.close();

  MPI_Finalize();
  return 0;
}
