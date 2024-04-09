#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include <grpc/grpc.h>
#include <iostream>
#include <memory>
#include <string>

#include "spark/connect/base.grpc.pb.h"
#include "spark/connect/base.pb.h"
#include "spark/connect/relations.pb.h"

using namespace spark::connect;

class SparkClient {
public:
  SparkClient(std::shared_ptr<grpc::Channel> channel)
      : stub_(SparkConnectService::NewStub(channel)) {}

  void readArrowData(const std::string &data) {
    auto buffer = std::make_shared<arrow::Buffer>(data);
    auto bufferReader = std::make_shared<arrow::io::BufferReader>(buffer);
    arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> result =
        arrow::ipc::RecordBatchStreamReader::Open(bufferReader);
    if (!result.ok()) {
      std::cerr << "Failed to open RecordBatchReader: " << result.status()
                << std::endl;
      return;
    }

    std::shared_ptr<arrow::ipc::RecordBatchReader> reader =
        result.ValueUnsafe();
    std::shared_ptr<arrow::Schema> schema = reader->schema();
    std::cout << "Schema: " << schema->ToString() << std::endl;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batchResult;
    while ((batchResult = reader->Next()).ok() && batchResult.ValueUnsafe()) {
      std::shared_ptr<arrow::RecordBatch> batch = batchResult.ValueUnsafe();
      std::cout << "Read a batch with " << batch->num_rows() << " rows and "
                << batch->num_columns() << " columns" << std::endl;
      std::cout << "Batch: " << batch->ToString() << std::endl;
    }
    if (!batchResult.ok()) {
      std::cerr << "Failed to read batch: " << batchResult.status()
                << std::endl;
    }
  }

  void executeSQL(const std::string &content, const std::string &sessionId) {
    auto sql = google::protobuf::Arena::CreateMessage<SQL>(&arena_);
    sql->set_query(content);

    auto relation = google::protobuf::Arena::CreateMessage<Relation>(&arena_);
    relation->set_allocated_sql(sql);

    auto plan = google::protobuf::Arena::CreateMessage<Plan>(&arena_);
    plan->set_allocated_root(relation);

    auto context = google::protobuf::Arena::CreateMessage<UserContext>(&arena_);
    context->set_user_id(kUserId);
    context->set_user_name(kUserName);

    auto request =
        google::protobuf::Arena::CreateMessage<ExecutePlanRequest>(&arena_);
    request->set_session_id(sessionId);
    request->set_allocated_user_context(context);
    request->set_allocated_plan(plan);
    auto reader = stub_->ExecutePlan(&context_, *request);
    ExecutePlanResponse response;
    while (reader->Read(&response)) {
      if (response.session_id() == sessionId && response.has_arrow_batch()) {
        const std::string &data = response.arrow_batch().data();
        readArrowData(data);
      }
    }
  }

private:
  std::unique_ptr<SparkConnectService::Stub> stub_;
  google::protobuf::Arena arena_;
  grpc::ClientContext context_;
  inline static constexpr std::string kUserId = "u0";
  inline static constexpr std::string kUserName = "native_client";
};

int main(int argc, char *argv[]) {
  auto channel = grpc::CreateChannel("localhost:15002",
                                     grpc::InsecureChannelCredentials());
  SparkClient client(channel);
  client.executeSQL("select cast('10.0' as decimal(20, 2))",
                    "70f50bc3-d60c-4ceb-8828-65de803561d8");
  return 0;
}
