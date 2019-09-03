#include "durability/single_node/state_delta.hpp"

#include <string>

#include "communication/bolt/v1/value.hpp"
#include "database/single_node/graph_db_accessor.hpp"
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

StateDelta StateDelta::CreateVertex(tx::TransactionId tx_id,
                                    storage::Gid vertex_id) {
  StateDelta op(StateDelta::Type::CREATE_VERTEX, tx_id);
  op.vertex_id = vertex_id;
  return op;
}

StateDelta StateDelta::CreateEdge(tx::TransactionId tx_id, storage::Gid edge_id,
                                  storage::Gid vertex_from_id,
                                  storage::Gid vertex_to_id,
                                  storage::EdgeType edge_type,
                                  const std::string &edge_type_name) {
  StateDelta op(StateDelta::Type::CREATE_EDGE, tx_id);
  op.edge_id = edge_id;
  op.vertex_from_id = vertex_from_id;
  op.vertex_to_id = vertex_to_id;
  op.edge_type = edge_type;
  op.edge_type_name = edge_type_name;
  return op;
}

StateDelta StateDelta::PropsSetVertex(tx::TransactionId tx_id,
                                      storage::Gid vertex_id,
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

StateDelta StateDelta::PropsSetEdge(tx::TransactionId tx_id,
                                    storage::Gid edge_id,
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

StateDelta StateDelta::AddLabel(tx::TransactionId tx_id, storage::Gid vertex_id,
                                storage::Label label,
                                const std::string &label_name) {
  StateDelta op(StateDelta::Type::ADD_LABEL, tx_id);
  op.vertex_id = vertex_id;
  op.label = label;
  op.label_name = label_name;
  return op;
}

StateDelta StateDelta::RemoveLabel(tx::TransactionId tx_id,
                                   storage::Gid vertex_id, storage::Label label,
                                   const std::string &label_name) {
  StateDelta op(StateDelta::Type::REMOVE_LABEL, tx_id);
  op.vertex_id = vertex_id;
  op.label = label;
  op.label_name = label_name;
  return op;
}

StateDelta StateDelta::RemoveVertex(tx::TransactionId tx_id,
                                    storage::Gid vertex_id, bool check_empty) {
  StateDelta op(StateDelta::Type::REMOVE_VERTEX, tx_id);
  op.vertex_id = vertex_id;
  op.check_empty = check_empty;
  return op;
}

StateDelta StateDelta::RemoveEdge(tx::TransactionId tx_id,
                                  storage::Gid edge_id) {
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

StateDelta StateDelta::DropIndex(tx::TransactionId tx_id, storage::Label label,
                                 const std::string &label_name,
                                 storage::Property property,
                                 const std::string &property_name) {
  StateDelta op(StateDelta::Type::DROP_INDEX, tx_id);
  op.label = label;
  op.label_name = label_name;
  op.property = property;
  op.property_name = property_name;
  return op;
}

StateDelta StateDelta::BuildUniqueConstraint(
    tx::TransactionId tx_id, storage::Label label,
    const std::string &label_name,
    const std::vector<storage::Property> &properties,
    const std::vector<std::string> &property_names) {
  StateDelta op(StateDelta::Type::BUILD_UNIQUE_CONSTRAINT, tx_id);
  op.label = label;
  op.label_name = label_name;
  op.properties = properties;
  op.property_names = property_names;
  return op;
}

StateDelta StateDelta::DropUniqueConstraint(
    tx::TransactionId tx_id, storage::Label label,
    const std::string &label_name,
    const std::vector<storage::Property> &properties,
    const std::vector<std::string> &property_names) {
  StateDelta op(StateDelta::Type::DROP_UNIQUE_CONSTRAINT, tx_id);
  op.label = label;
  op.label_name = label_name;
  op.properties = properties;
  op.property_names = property_names;
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
      encoder.WriteInt(vertex_id.AsInt());
      break;
    case Type::CREATE_EDGE:
      encoder.WriteInt(edge_id.AsInt());
      encoder.WriteInt(vertex_from_id.AsInt());
      encoder.WriteInt(vertex_to_id.AsInt());
      encoder.WriteInt(edge_type.Id());
      encoder.WriteString(edge_type_name);
      break;
    case Type::SET_PROPERTY_VERTEX:
      encoder.WriteInt(vertex_id.AsInt());
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      encoder.WriteValue(glue::ToBoltValue(value));
      break;
    case Type::SET_PROPERTY_EDGE:
      encoder.WriteInt(edge_id.AsInt());
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      encoder.WriteValue(glue::ToBoltValue(value));
      break;
    case Type::ADD_LABEL:
    case Type::REMOVE_LABEL:
      encoder.WriteInt(vertex_id.AsInt());
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      break;
    case Type::REMOVE_VERTEX:
      encoder.WriteInt(vertex_id.AsInt());
      break;
    case Type::REMOVE_EDGE:
      encoder.WriteInt(edge_id.AsInt());
      break;
    case Type::BUILD_INDEX:
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      break;
    case Type::DROP_INDEX:
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      encoder.WriteInt(property.Id());
      encoder.WriteString(property_name);
      break;
    case Type::BUILD_UNIQUE_CONSTRAINT:
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      encoder.WriteInt(properties.size());
      for (auto prop : properties) {
        encoder.WriteInt(prop.Id());
      }
      for (auto &name : property_names) {
        encoder.WriteString(name);
      }
      break;
    case Type::DROP_UNIQUE_CONSTRAINT:
      encoder.WriteInt(label.Id());
      encoder.WriteString(label_name);
      encoder.WriteInt(properties.size());
      for (auto prop : properties) {
        encoder.WriteInt(prop.Id());
      }
      for (auto &name : property_names) {
        encoder.WriteString(name);
      }
      break;
  }

  writer.WriteValue(writer.hash());
}

#define DECODE_MEMBER(member, value_f)         \
  if (!decoder.ReadValue(&dv)) return nullopt; \
  r_val.member = dv.value_f();

#define DECODE_GID_MEMBER(member)              \
  if (!decoder.ReadValue(&dv)) return nullopt; \
  r_val.member = storage::Gid::FromInt(dv.ValueInt());

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
        DECODE_GID_MEMBER(vertex_id)
        break;
      case Type::CREATE_EDGE:
        DECODE_GID_MEMBER(edge_id)
        DECODE_GID_MEMBER(vertex_from_id)
        DECODE_GID_MEMBER(vertex_to_id)
        DECODE_MEMBER_CAST(edge_type, ValueInt, storage::EdgeType)
        DECODE_MEMBER(edge_type_name, ValueString)
        break;
      case Type::SET_PROPERTY_VERTEX:
        DECODE_GID_MEMBER(vertex_id)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        r_val.value = glue::ToPropertyValue(dv);
        break;
      case Type::SET_PROPERTY_EDGE:
        DECODE_GID_MEMBER(edge_id)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        r_val.value = glue::ToPropertyValue(dv);
        break;
      case Type::ADD_LABEL:
      case Type::REMOVE_LABEL:
        DECODE_GID_MEMBER(vertex_id)
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        break;
      case Type::REMOVE_VERTEX:
        DECODE_GID_MEMBER(vertex_id)
        break;
      case Type::REMOVE_EDGE:
        DECODE_GID_MEMBER(edge_id)
        break;
      case Type::BUILD_INDEX:
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        break;
      case Type::DROP_INDEX:
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        DECODE_MEMBER_CAST(property, ValueInt, storage::Property)
        DECODE_MEMBER(property_name, ValueString)
        break;
      case Type::BUILD_UNIQUE_CONSTRAINT: {
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        int size = dv.ValueInt();
        for (size_t i = 0; i < size; ++i) {
          if (!decoder.ReadValue(&dv)) return nullopt;
          r_val.properties.push_back(
              static_cast<storage::Property>(dv.ValueInt()));
        }
        for (size_t i = 0; i < size; ++i) {
          if (!decoder.ReadValue(&dv)) return nullopt;
          r_val.property_names.push_back(dv.ValueString());
        }
        break;
      }
      case Type::DROP_UNIQUE_CONSTRAINT: {
        DECODE_MEMBER_CAST(label, ValueInt, storage::Label)
        DECODE_MEMBER(label_name, ValueString)
        if (!decoder.ReadValue(&dv)) return nullopt;
        int size = dv.ValueInt();
        for (size_t i = 0; i < size; ++i) {
          if (!decoder.ReadValue(&dv)) return nullopt;
          r_val.properties.push_back(
              static_cast<storage::Property>(dv.ValueInt()));
        }
        for (size_t i = 0; i < size; ++i) {
          if (!decoder.ReadValue(&dv)) return nullopt;
          r_val.property_names.push_back(dv.ValueString());
        }
        break;
      }
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
      dba.InsertVertex(vertex_id);
      break;
    case Type::CREATE_EDGE: {
      auto from = dba.FindVertex(vertex_from_id, true);
      auto to = dba.FindVertex(vertex_to_id, true);
      dba.InsertEdge(from, to, dba.EdgeType(edge_type_name), edge_id);
      break;
    }
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
    case Type::BUILD_INDEX:
    case Type::DROP_INDEX: {
      LOG(FATAL) << "Index handling not handled in Apply";
      break;
    }
    case Type::BUILD_UNIQUE_CONSTRAINT: {
      std::vector<storage::Property> properties;
      properties.reserve(property_names.size());
      for (auto &p : property_names) {
        properties.push_back(dba.Property(p));
      }

      dba.BuildUniqueConstraint(dba.Label(label_name), properties);
    } break;
    case Type::DROP_UNIQUE_CONSTRAINT: {
      std::vector<storage::Property> properties;
      properties.reserve(property_names.size());
      for (auto &p : property_names) {
        properties.push_back(dba.Property(p));
      }

      dba.DeleteUniqueConstraint(dba.Label(label_name), properties);
    } break;
  }
}

};  // namespace database
