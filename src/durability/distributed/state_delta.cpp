#include "durability/distributed/state_delta.hpp"

#include <string>

#include "communication/bolt/v1/value.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "glue/communication.hpp"

namespace database {

StateDelta StateDelta::TxBegin(tx::TransactionId tx_id) {
  return {StateDelta::Type::TRANSACTION_BEGIN, tx_id};
}

StateDelta StateDelta::TxCommit(tx::TransactionId tx_id) {
  return {StateDelta::Type::TRANSACTION_COMMIT, tx_id};
}

StateDelta StateDelta::TxAbort(tx::TransactionId tx_id) {
  return {StateDelta::Type::TRANSACTION_ABORT, tx_id};
}

StateDelta StateDelta::CreateVertex(tx::TransactionId tx_id, gid::Gid vertex_id,
                                    int64_t cypher_id) {
  StateDelta op(StateDelta::Type::CREATE_VERTEX, tx_id);
  op.vertex_id = vertex_id;
  op.cypher_id = cypher_id;
  return op;
}

StateDelta StateDelta::CreateEdge(tx::TransactionId tx_id, gid::Gid edge_id,
                                  int64_t cypher_id, gid::Gid vertex_from_id,
                                  gid::Gid vertex_to_id,
                                  storage::EdgeType edge_type,
                                  const std::string &edge_type_name) {
  StateDelta op(StateDelta::Type::CREATE_EDGE, tx_id);
  op.edge_id = edge_id;
  op.cypher_id = cypher_id;
  op.vertex_from_id = vertex_from_id;
  op.vertex_to_id = vertex_to_id;
  op.edge_type = edge_type;
  op.edge_type_name = edge_type_name;
  return op;
}

StateDelta StateDelta::AddOutEdge(tx::TransactionId tx_id, gid::Gid vertex_id,
                                  storage::VertexAddress vertex_to_address,
                                  storage::EdgeAddress edge_address,
                                  storage::EdgeType edge_type) {
  CHECK(vertex_to_address.is_remote() && edge_address.is_remote())
      << "WAL can only contain global addresses.";
  StateDelta op(StateDelta::Type::ADD_OUT_EDGE, tx_id);
  op.vertex_id = vertex_id;
  op.vertex_to_address = vertex_to_address;
  op.edge_address = edge_address;
  op.edge_type = edge_type;
  return op;
}

StateDelta StateDelta::RemoveOutEdge(tx::TransactionId tx_id,
                                     gid::Gid vertex_id,
                                     storage::EdgeAddress edge_address) {
  CHECK(edge_address.is_remote()) << "WAL can only contain global addresses.";
  StateDelta op(StateDelta::Type::REMOVE_OUT_EDGE, tx_id);
  op.vertex_id = vertex_id;
  op.edge_address = edge_address;
  return op;
}

StateDelta StateDelta::AddInEdge(tx::TransactionId tx_id, gid::Gid vertex_id,
                                 storage::VertexAddress vertex_from_address,
                                 storage::EdgeAddress edge_address,
                                 storage::EdgeType edge_type) {
  CHECK(vertex_from_address.is_remote() && edge_address.is_remote())
      << "WAL can only contain global addresses.";
  StateDelta op(StateDelta::Type::ADD_IN_EDGE, tx_id);
  op.vertex_id = vertex_id;
  op.vertex_from_address = vertex_from_address;
  op.edge_address = edge_address;
  op.edge_type = edge_type;
  return op;
}

StateDelta StateDelta::RemoveInEdge(tx::TransactionId tx_id, gid::Gid vertex_id,
                                    storage::EdgeAddress edge_address) {
  CHECK(edge_address.is_remote()) << "WAL can only contain global addresses.";
  StateDelta op(StateDelta::Type::REMOVE_IN_EDGE, tx_id);
  op.vertex_id = vertex_id;
  op.edge_address = edge_address;
  return op;
}

StateDelta StateDelta::PropsSetVertex(tx::TransactionId tx_id,
                                      gid::Gid vertex_id,
                                      storage::Property property,
                                      const std::string &property_name,
                                      const PropertyValue &value) {
  StateDelta op(StateDelta::Type::SET_PROPERTY_VERTEX, tx_id);
  op.vertex_id = vertex_id;
  op.property = property;
  op.property_name = property_name;
  op.value = value;
  return op;
}

StateDelta StateDelta::PropsSetEdge(tx::TransactionId tx_id, gid::Gid edge_id,
                                    storage::Property property,
                                    const std::string &property_name,
                                    const PropertyValue &value) {
  StateDelta op(StateDelta::Type::SET_PROPERTY_EDGE, tx_id);
  op.edge_id = edge_id;
  op.property = property;
  op.property_name = property_name;
  op.value = value;
  return op;
}

StateDelta StateDelta::AddLabel(tx::TransactionId tx_id, gid::Gid vertex_id,
                                storage::Label label,
                                const std::string &label_name) {
  StateDelta op(StateDelta::Type::ADD_LABEL, tx_id);
  op.vertex_id = vertex_id;
  op.label = label;
  op.label_name = label_name;
  return op;
}

StateDelta StateDelta::RemoveLabel(tx::TransactionId tx_id, gid::Gid vertex_id,
                                   storage::Label label,
                                   const std::string &label_name) {
  StateDelta op(StateDelta::Type::REMOVE_LABEL, tx_id);
  op.vertex_id = vertex_id;
  op.label = label;
  op.label_name = label_name;
  return op;
}

StateDelta StateDelta::RemoveVertex(tx::TransactionId tx_id, gid::Gid vertex_id,
                                    bool check_empty) {
  StateDelta op(StateDelta::Type::REMOVE_VERTEX, tx_id);
  op.vertex_id = vertex_id;
  op.check_empty = check_empty;
  return op;
}

StateDelta StateDelta::RemoveEdge(tx::TransactionId tx_id, gid::Gid edge_id) {
  StateDelta op(StateDelta::Type::REMOVE_EDGE, tx_id);
  op.edge_id = edge_id;
  return op;
}

StateDelta StateDelta::BuildIndex(tx::TransactionId tx_id, storage::Label label,
                                  const std::string &label_name,
                                  storage::Property property,
                                  const std::string &property_name) {
  StateDelta op(StateDelta::Type::BUILD_INDEX, tx_id);
  op.label = label;
  op.label_name = label_name;
  op.property = property;
  op.property_name = property_name;
  return op;
}

void StateDelta::Encode(
    HashedFileWriter &writer,
    communication::bolt::BaseEncoder<HashedFileWriter> &encoder) const {
  encoder.WriteInt(static_cast<int64_t>(type));
  encoder.WriteInt(static_cast<int64_t>(transaction_id));

  switch (type) {
    case Type::TRANSACTION_BEGIN:
    case Type::TRANSACTION_COMMIT:
    case Type::TRANSACTION_ABORT:
      break;
    case Type::CREATE_VERTEX:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(cypher_id);
      break;
    case Type::CREATE_EDGE:
      encoder.WriteInt(edge_id);
      encoder.WriteInt(cypher_id);
      encoder.WriteInt(vertex_from_id);
      encoder.WriteInt(vertex_to_id);
      encoder.WriteInt(edge_type.Id());
      encoder.WriteString(edge_type_name);
      break;
    case Type::ADD_OUT_EDGE:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(vertex_to_address.raw());
      encoder.WriteInt(edge_address.raw());
      encoder.WriteInt(edge_type.Id());
      break;
    case Type::REMOVE_OUT_EDGE:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(edge_address.raw());
      break;
    case Type::ADD_IN_EDGE:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(vertex_from_address.raw());
      encoder.WriteInt(edge_address.raw());
      encoder.WriteInt(edge_type.Id());
      break;
    case Type::REMOVE_IN_EDGE:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(edge_address.raw());
      break;
    case Type::SET_PROPERTY_VERTEX:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      encoder.WriteValue(glue::ToBoltValue(value));
      break;
    case Type::SET_PROPERTY_EDGE:
      encoder.WriteInt(edge_id);
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      encoder.WriteValue(glue::ToBoltValue(value));
      break;
    case Type::ADD_LABEL:
    case Type::REMOVE_LABEL:
      encoder.WriteInt(vertex_id);
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      break;
    case Type::REMOVE_VERTEX:
      encoder.WriteInt(vertex_id);
      break;
    case Type::REMOVE_EDGE:
      encoder.WriteInt(edge_id);
      break;
    case Type::BUILD_INDEX:
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      break;
  }

  writer.WriteValue(writer.hash());
}

#define DECODE_MEMBER(member, value_f)         \
  if (!decoder.ReadValue(&dv)) return nullopt; \
  r_val.member = dv.value_f();

#define DECODE_MEMBER_CAST(member, value_f, type) \
  if (!decoder.ReadValue(&dv)) return nullopt;    \
  r_val.member = static_cast<type>(dv.value_f());

std::optional<StateDelta> StateDelta::Decode(
    HashedFileReader &reader,
    communication::bolt::Decoder<HashedFileReader> &decoder) {
  using std::nullopt;

  StateDelta r_val;
  // The decoded value used as a temporary while decoding.
  communication::bolt::Value dv;

  try {
    if (!decoder.ReadValue(&dv)) return nullopt;
    r_val.type = static_cast<enum StateDelta::Type>(dv.ValueInt());
    DECODE_MEMBER(transaction_id, ValueInt)

    switch (r_val.type) {
      case Type::TRANSACTION_BEGIN:
      case Type::TRANSACTION_COMMIT:
      case Type::TRANSACTION_ABORT:
        break;
      case Type::CREATE_VERTEX:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER(cypher_id, ValueInt)
        break;
      case Type::CREATE_EDGE:
        DECODE_MEMBER(edge_id, ValueInt)
        DECODE_MEMBER(cypher_id, ValueInt)
        DECODE_MEMBER(vertex_from_id, ValueInt)
        DECODE_MEMBER(vertex_to_id, ValueInt)
        DECODE_MEMBER_CAST(edge_type, ValueInt, storage::EdgeType)
        DECODE_MEMBER(edge_type_name, ValueString)
        break;
      case Type::ADD_OUT_EDGE:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(vertex_to_address, ValueInt, storage::VertexAddress)
        DECODE_MEMBER_CAST(edge_address, ValueInt, storage::EdgeAddress)
        DECODE_MEMBER_CAST(edge_type, ValueInt, storage::EdgeType)
        break;
      case Type::REMOVE_OUT_EDGE:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(edge_address, ValueInt, storage::EdgeAddress)
        break;
      case Type::ADD_IN_EDGE:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(vertex_from_address, ValueInt,
                           storage::VertexAddress)
        DECODE_MEMBER_CAST(edge_address, ValueInt, storage::EdgeAddress)
        DECODE_MEMBER_CAST(edge_type, ValueInt, storage::EdgeType)
        break;
      case Type::REMOVE_IN_EDGE:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(edge_address, ValueInt, storage::EdgeAddress)
        break;
      case Type::SET_PROPERTY_VERTEX:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        r_val.value = glue::ToPropertyValue(dv);
        break;
      case Type::SET_PROPERTY_EDGE:
        DECODE_MEMBER(edge_id, ValueInt)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        r_val.value = glue::ToPropertyValue(dv);
        break;
      case Type::ADD_LABEL:
      case Type::REMOVE_LABEL:
        DECODE_MEMBER(vertex_id, ValueInt)
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        break;
      case Type::REMOVE_VERTEX:
        DECODE_MEMBER(vertex_id, ValueInt)
        break;
      case Type::REMOVE_EDGE:
        DECODE_MEMBER(edge_id, ValueInt)
        break;
      case Type::BUILD_INDEX:
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        break;
    }

    auto decoder_hash = reader.hash();
    uint64_t encoded_hash;
    if (!reader.ReadType(encoded_hash, true)) return nullopt;
    if (decoder_hash != encoded_hash) return nullopt;

    return r_val;
  } catch (communication::bolt::ValueException &) {
    return nullopt;
  } catch (std::ifstream::failure &) {
    return nullopt;
  }
}

#undef DECODE_MEMBER

void StateDelta::Apply(GraphDbAccessor &dba) const {
  switch (type) {
    // Transactional state is not recovered.
    case Type::TRANSACTION_BEGIN:
    case Type::TRANSACTION_COMMIT:
    case Type::TRANSACTION_ABORT:
      LOG(FATAL) << "Transaction handling not handled in Apply";
      break;
    case Type::CREATE_VERTEX:
      dba.InsertVertex(vertex_id, cypher_id);
      break;
    case Type::CREATE_EDGE: {
      auto from = dba.FindVertex(vertex_from_id, true);
      auto to = dba.FindVertex(vertex_to_id, true);
      dba.InsertEdge(from, to, dba.EdgeType(edge_type_name), edge_id,
                     cypher_id);
      break;
    }
    case Type::ADD_OUT_EDGE:
    case Type::REMOVE_OUT_EDGE:
    case Type::ADD_IN_EDGE:
    case Type::REMOVE_IN_EDGE:
      LOG(FATAL) << "Partial edge creation/deletion not yet supported in Apply";
    case Type::SET_PROPERTY_VERTEX: {
      auto vertex = dba.FindVertex(vertex_id, true);
      vertex.PropsSet(dba.Property(property_name), value);
      break;
    }
    case Type::SET_PROPERTY_EDGE: {
      auto edge = dba.FindEdge(edge_id, true);
      edge.PropsSet(dba.Property(property_name), value);
      break;
    }
    case Type::ADD_LABEL: {
      auto vertex = dba.FindVertex(vertex_id, true);
      vertex.add_label(dba.Label(label_name));
      break;
    }
    case Type::REMOVE_LABEL: {
      auto vertex = dba.FindVertex(vertex_id, true);
      vertex.remove_label(dba.Label(label_name));
      break;
    }
    case Type::REMOVE_VERTEX: {
      auto vertex = dba.FindVertex(vertex_id, true);
      dba.DetachRemoveVertex(vertex);
      break;
    }
    case Type::REMOVE_EDGE: {
      auto edge = dba.FindEdge(edge_id, true);
      dba.RemoveEdge(edge);
      break;
    }
    case Type::BUILD_INDEX: {
      LOG(FATAL) << "Index handling not handled in Apply";
      break;
    }
  }
}

};  // namespace database