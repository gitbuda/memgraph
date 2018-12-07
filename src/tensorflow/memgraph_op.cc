/**
 * \file memgraph_op.cc
 * \brief Implementation of a memgraph operation in Tensorflow.
 */

#include <string>
#include <vector>

#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/platform/default/logging.h"

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

using namespace tensorflow;

const string kHost = "host";
const string kPort = "port";
const string kUser = "user";
const string kPassword = "password";
const string kUseSsl = "use_ssl";
const string kInputList = "input_list";

/**
 * @brief Construct attribute definition.
 * @details Attribute in TF has special format "key: value = default". This
 * function is helper function for constructing attribute definition.
 *
 * @param key Attribute name
 * @param value Attribute value
 * @param default_value Default value
 * @return "key: value = default_value"
 */
const string Define(const string& key, const string& value,
                    const string& default_value) {
  return key + ": " + value + " = " + default_value;
}

REGISTER_OP("MemgraphOp")
    .Attr(Define(kHost, "string", "'127.0.0.1'"))
    .Attr(Define(kPort, "int", "7687"))
    .Attr(Define(kUser, "string", "''"))
    .Attr(Define(kPassword, "string", "''"))
    .Attr(Define(kUseSsl, "bool", "false"))
    .Attr("T: {int64, double, bool, string}")
    .Input("query: string")
    .Input("input_list: int64")
    .Output("header: string")
    .Output("rows: T")
    .SetShapeFn([](::tensorflow::shape_inference::InferenceContext* c) {
      ::tensorflow::shape_inference::ShapeHandle input;
      TF_RETURN_IF_ERROR(c->WithRank(c->input(0), 0, &input));
      TF_RETURN_IF_ERROR(c->WithRank(c->input(1), 1, &input));
      return Status::OK();
    });

// TODO Smart connecting to memgraph
/**
 * @brief Memgraph Tensorflow Op
 * @details Memgraph op is the wrapper around the memgraph client. Memgraph op
 * takes attributes for connection information and one attribute for output type
 * definition: int64, double, bool, string. There are two inputs: query (string)
 * and the input list (int64 list). The user can use the input list in the query
 * with variable $input_list. There are two outputs: header and rows. Headers
 * are names of the columns in the output table and rows are all data fetch from
 * memgraph with the query. Memgraph Op has one limitation on output. All output
 * values in rows data must have the same type. If the user set output type to
 * string, then all data convert to the string,  however, in any other case
 * error appears  (the implicit cast is not possible for other types).
 *
 *
 * @tparam T Output type
 */
template <typename T>
class MemgraphOp : public OpKernel {
 private:
  const string kBoltClientVersion =
      "TensorflowClient";  // TODO maybe we can use real version...
  string host_;
  int port_;
  string user_;
  string password_;
  bool use_ssl_;
  communication::bolt::Client* client_;

  T GetValue(const communication::bolt::Value& value, OpKernelContext* context);

 public:
  /**
   * @brief Constructor
   * @details Instance will connect to memgraph.
   *
   * @param context Context contains attribute values - database connection
   * information and output data type.
   */
  explicit MemgraphOp(OpKernelConstruction* context) : OpKernel(context) {
    OP_REQUIRES_OK(context, context->GetAttr(kHost, &host_));
    OP_REQUIRES_OK(context, context->GetAttr(kPort, &port_));
    OP_REQUIRES_OK(context, context->GetAttr(kUser, &user_));
    OP_REQUIRES_OK(context, context->GetAttr(kPassword, &password_));
    OP_REQUIRES_OK(context, context->GetAttr(kUseSsl, &use_ssl_));
    communication::Init();
    io::network::Endpoint endpoint(io::network::ResolveHostname(host_), port_);
    communication::ClientContext context_mg(use_ssl_);
    client_ = new communication::bolt::Client(&context_mg);
    OP_REQUIRES(context, client_ != NULL,
                errors::Internal("Cannot create client"));

    client_->Connect(endpoint, user_, password_, kBoltClientVersion);
  }

  /**
   * @brief Compute stage
   * @details Op reads input data (query and input list), executes query and on
   * the end fills outputs.
   *
   * @param context Context contains data for inputs and outputs.
   */
  void Compute(OpKernelContext* context) override {
    const Tensor& param_tensor = context->input(1);
    auto params = param_tensor.flat<int64>();
    std::vector<communication::bolt::Value> input_list;

    for (int i = 0; i < params.size(); ++i) {
      communication::bolt::Value value(static_cast<int64_t>(params(i)));
      input_list.push_back(value);
    }

    bool exception_free = true;
    string message;
    try {
      const Tensor& input_tensor = context->input(0);
      auto query = input_tensor.flat<string>()(0);

      auto ret = client_->Execute(query, {{kInputList, input_list}});

      TensorShape header_output__shape;
      header_output__shape.AddDim(ret.fields.size());
      Tensor* header_output = NULL;
      OP_REQUIRES_OK(context, context->allocate_output(0, header_output__shape,
                                                       &header_output));
      auto header_output_flat = header_output->flat<string>();

      TensorShape rows_output_shape;
      rows_output_shape.AddDim(ret.records.size());
      rows_output_shape.AddDim(ret.fields.size());
      Tensor* rows_output = NULL;

      OP_REQUIRES_OK(context, context->allocate_output(1, rows_output_shape,
                                                       &rows_output));
      auto rows_output_matrix = rows_output->matrix<T>();

      for (int i = 0; i < ret.fields.size(); ++i) {
        header_output_flat(i) = ret.fields[i];
      }

      for (int i = 0; i < ret.records.size(); ++i) {
        for (int j = 0; j < ret.records[i].size(); ++j) {
          const auto& field = ret.records[i][j];
          try {
            rows_output_matrix(i, j) = GetValue(field, context);
          } catch (const communication::bolt::ValueException e) {
            std::stringstream value_stream;
            value_stream << field;
            std::stringstream type_stream;
            type_stream << field.type();
            string message = "Wrong type: " + header_output_flat(i) + " = " +
                             type_stream.str() + "(" + value_stream.str() + ")";
            OP_REQUIRES(context, false, errors::Internal(message));
          }
        }
      }
    } catch (const communication::bolt::ClientFatalException& e) {
      client_->Close();
      exception_free = false;
      message = e.what();
    } catch (communication::bolt::ClientQueryException& e) {
      client_->Close();
      exception_free = false;
      message = e.what();
    }
    OP_REQUIRES(context, exception_free, errors::Internal(message));
  }
};

template <>
int64 MemgraphOp<int64>::GetValue(const communication::bolt::Value& value,
                                  OpKernelContext* context) {
  return value.ValueInt();
}

template <>
double MemgraphOp<double>::GetValue(const communication::bolt::Value& value,
                                    OpKernelContext* context) {
  return value.ValueDouble();
}

template <>
bool MemgraphOp<bool>::GetValue(const communication::bolt::Value& value,
                                OpKernelContext* context) {
  return value.ValueBool();
}

template <>
string MemgraphOp<string>::GetValue(const communication::bolt::Value& value,
                                    OpKernelContext* context) {
  std::stringstream value_stream;
  value_stream << value;
  return value_stream.str();
}

REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<int64>("T"),
    MemgraphOp<int64>);
REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<double>("T"),
    MemgraphOp<double>);
REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<bool>("T"),
    MemgraphOp<bool>);
REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<string>("T"),
    MemgraphOp<string>);