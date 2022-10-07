// Copyright 2018 Google LLC
// Copyright 2018-present Open Networking Foundation
// SPDX-License-Identifier: Apache-2.0

#ifndef STRATUM_HAL_LIB_PHAL_ATTRIBUTE_DATABASE_INTERFACE_H_
#define STRATUM_HAL_LIB_PHAL_ATTRIBUTE_DATABASE_INTERFACE_H_

#include <functional>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/types/variant.h"
#include "google/protobuf/descriptor.h"
#include "stratum/glue/integral_types.h"
#include "stratum/glue/status/statusor.h"
#include "stratum/hal/lib/phal/db.pb.h"
#include "stratum/lib/channel/channel.h"

namespace stratum {
namespace hal {
namespace phal {
using Attribute =
    absl::variant<char, int32, int64, uint32, uint64, float, double, bool,
                  std::string, const google::protobuf::EnumValueDescriptor*>;

// TODO(unknown): Add an 'optional' flag to PathEntry. This flag indicates
// that it is okay to skip querying this path if the marked PathEntry is not
// present in the database.
struct PathEntry {
  PathEntry(const std::string& name, int index, bool indexed, bool all,
            bool terminal_group)
      : name(name),
        index(index),
        indexed(indexed),
        all(all),
        terminal_group(terminal_group) {}
  // Constructs a PathEntry for an attribute with an empty name. This PathEntry
  // is not valid, and its fields must be set after construction.
  PathEntry() : PathEntry("", -1, false, false, false) {}
  // Constucts a PathEntry for an attribute with the given name.
  explicit PathEntry(const std::string& name)
      : PathEntry(name, -1, false, false, false) {}
  // Constructs a PathEntry for the given index into a repeated attribute group.
  PathEntry(const std::string& name, int index)
      : PathEntry(name, index, true, false, false) {}

  std::string name;
  int index = -1;        // This field is only significant if indexed == true.
  bool indexed = false;  // If true, this is a repeated attribute group.
  bool all = false;  // If true, this is a repeated group, but ignore index and
                     // fetch all indices.
  bool terminal_group = false;  // In true, fetch below this attribute group.

  bool operator<(const PathEntry& other) const {
    if (name != other.name) return name < other.name;
    return index < other.index;
  }

  bool operator==(const PathEntry& other) const {
    return name == other.name && index == other.index &&
           indexed == other.indexed && all == other.all &&
           terminal_group == other.terminal_group;
  }

  template <typename H>
  friend H AbslHashValue(H h, const PathEntry& p);
};

// This method makes PathEntry hashable by Abseil's default hash function.
template <typename H>
H AbslHashValue(H h, const PathEntry& p) {
  return H::combine(std::move(h), p.name);
}

typedef std::vector<PathEntry> Path;

// A map used when setting values in the attribute database.
using AttributeValueMap = absl::flat_hash_map<Path, Attribute>;

// A single query into an attribute database, generated by calling
// AttributeDatabaseInterface::MakeQuery. Queries the set of database paths
// passed into MakeQuery.
class Query {
 public:
  virtual ~Query() {}
  // Returns a proto containing any requested fields in the database. All query
  // fields are considered optional, so missing fields are not populated. Note
  // that subsequent calls to Get() will query the system for attribute values
  // multiple times, and may return different results.
  virtual ::util::StatusOr<std::unique_ptr<PhalDB>> Get() = 0;
  // Subscribes to changes in the result of this query. A message will
  // immediately be sent with the initial value of the query. Subsequent
  // messages are sent whenever the result of the query changes, with an effort
  // to ensure that no longer than the given polling_interval elapses before a
  // change is noticed. If no change has occurred after the polling interval, a
  // message is not sent. If a message would be sent but the channel buffer is
  // full, the message is dropped. Note that messages may be sent more
  // frequently than the given polling interval.
  virtual ::util::Status Subscribe(
      std::unique_ptr<ChannelWriter<PhalDB>> subscriber,
      absl::Duration polling_interval) = 0;

 protected:
  Query() {}
};

// Allows simple access to a database of system attributes.
//
// This interface is likely to change significantly to support better error
// reporting, and/or other unforseen use cases.
class AttributeDatabaseInterface {
 public:
  virtual ~AttributeDatabaseInterface() {}
  // TODO(unknown): Implement and document Set. This interface will likely
  // change.
  virtual ::util::Status Set(const AttributeValueMap& values) = 0;
  // Creates a new query that reads the given query paths. The results of this
  // query may be accessed by calling Get() or Subscribe(...) on the returned
  // Query. If a query is returned, this query will remain valid until it is
  // deleted, and values returned by Get() will reflect any updates to the
  // database structure.
  virtual ::util::StatusOr<std::unique_ptr<Query>> MakeQuery(
      const std::vector<Path>& query_paths) = 0;

 protected:
  AttributeDatabaseInterface() {}
};
}  // namespace phal
}  // namespace hal
}  // namespace stratum

#endif  // STRATUM_HAL_LIB_PHAL_ATTRIBUTE_DATABASE_INTERFACE_H_
