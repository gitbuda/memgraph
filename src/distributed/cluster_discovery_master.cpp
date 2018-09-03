#include "distributed/cluster_discovery_master.hpp"

#include <experimental/filesystem>

#include "communication/rpc/client_pool.hpp"
#include "distributed/coordination_rpc_messages.hpp"
#include "io/network/endpoint.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

namespace distributed {
using Server = communication::rpc::Server;

ClusterDiscoveryMaster::ClusterDiscoveryMaster(
    Server &server, MasterCoordination &coordination,
    RpcWorkerClients &rpc_worker_clients,
    const std::string &durability_directory)
    : server_(server),
      coordination_(coordination),
      rpc_worker_clients_(rpc_worker_clients),
      durability_directory_(durability_directory) {
  server_.Register<RegisterWorkerRpc>([this](const auto &endpoint,
                                             const auto &req_reader,
                                             auto *res_builder) {
    bool registration_successful = false;
    bool durability_error = false;

    RegisterWorkerReq req;
    req.Load(req_reader);

    // Compose the worker's endpoint from its connecting address and its
    // advertised port.
    io::network::Endpoint worker_endpoint(endpoint.address(), req.port);

    // Create and find out what is our durability directory.
    CHECK(utils::EnsureDir(durability_directory_))
        << "Couldn't create durability directory '" << durability_directory_
        << "'!";
    auto full_durability_directory =
        std::experimental::filesystem::canonical(durability_directory_);

    // Check whether the worker is running on the same host (detected when it
    // connects to us over the loopback interface) and whether it has the same
    // durability directory as us.
    // TODO (mferencevic): This check should also be done for all workers in
    // between them because this check only verifies that the worker and master
    // don't collide, there can still be a collision between workers.
    if ((utils::StartsWith(endpoint.address(), "127.") ||
         endpoint.address() == "::1") &&
        req.durability_directory == full_durability_directory) {
      durability_error = true;
      LOG(WARNING)
          << "The worker at " << worker_endpoint
          << " was started with the same durability directory as the master!";
    }

    // Register the worker if the durability check succeeded.
    if (!durability_error) {
      registration_successful = this->coordination_.RegisterWorker(
          req.desired_worker_id, worker_endpoint);
    }

    // Notify the cluster of the new worker if the registration succeeded.
    if (registration_successful) {
      rpc_worker_clients_.ExecuteOnWorkers<void>(
          0, [req, worker_endpoint](
                 int worker_id, communication::rpc::ClientPool &client_pool) {
            auto result = client_pool.Call<ClusterDiscoveryRpc>(
                req.desired_worker_id, worker_endpoint);
            CHECK(result) << "ClusterDiscoveryRpc failed";
          });
    }

    RegisterWorkerRes res(registration_successful, durability_error,
                          this->coordination_.RecoveredSnapshotTx(),
                          this->coordination_.GetWorkers());
    res.Save(res_builder);
  });

  server_.Register<NotifyWorkerRecoveredRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        NotifyWorkerRecoveredReq req;
        req.Load(req_reader);
        this->coordination_.WorkerRecoveredSnapshot(req.worker_id,
                                                    req.recovery_info);
      });
}

}  // namespace distributed
