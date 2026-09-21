#include "worker_coef_estimator.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <cmath>

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr double SolverTolerance = 1e-10;
constexpr int SolverMaxSweeps = 1000;

bool Touches(const TWorkerCoefEdge& edge, const std::string& worker)
{
    return edge.From == worker || edge.To == worker;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TWorkerCoefEstimator::TWorkerCoefEstimator(TBalancerGroupStatePtr state, TWorkerCoefEstimatorConfig config)
    : State_(std::move(state))
    , Config_(config)
{ }

std::optional<double> TWorkerCoefEstimator::MakeObservation(double cpuA, double rpsA, double cpuB, double rpsB) const
{
    if (cpuA < Config_.MinCpuUsage || cpuB < Config_.MinCpuUsage || rpsA <= 0. || rpsB <= 0.) {
        return std::nullopt;
    }
    double rateRatio = rpsB / rpsA;
    if (rateRatio > Config_.MaxRateRatio || rateRatio < 1. / Config_.MaxRateRatio) {
        return std::nullopt;
    }
    // CPU per message: a backlog catch-up after the move inflates CPU and rate alike.
    return std::log((cpuB / rpsB) / (cpuA / rpsA));
}

void TWorkerCoefEstimator::AddObservation(const std::string& from, const std::string& to, double obs, double weight, TInstant now)
{
    if (from == to || weight <= 0.) {
        return;
    }
    // An edge is a pair, not a direction: store it as From < To.
    if (to < from) {
        AddObservation(to, from, -obs, weight, now);
        return;
    }

    auto& edges = State_->WorkerCoefEdges[from];
    auto it = edges.find(to);
    if (it == edges.end()) {
        TWorkerCoefEdge edge;
        edge.From = from;
        edge.To = to;
        edge.Obs = obs;
        edge.Weight = weight;
        edge.UpdatedAt = now;
        edges.emplace(to, std::move(edge));
        EnforceEdgeLimit(from);
        EnforceEdgeLimit(to);
        return;
    }

    // Aging happens only here, so that without new observations the estimate stays as it is,
    // while after a long pause the new observation mostly replaces the stale one.
    auto& edge = it->second;
    double decay = std::exp2(-std::max(0., (now - edge.UpdatedAt).SecondsFloat()) / Config_.HalfLife.SecondsFloat());
    double oldWeight = edge.Weight * decay;
    edge.Weight = oldWeight + weight;
    edge.Obs = (edge.Obs * oldWeight + obs * weight) / edge.Weight;
    edge.UpdatedAt = now;
}

void TWorkerCoefEstimator::EnforceEdgeLimit(const std::string& worker)
{
    // Runs only when an edge is created; the edges of one worker are found by a full scan.
    while (true) {
        const TWorkerCoefEdge* oldest = nullptr;
        int count = 0;
        for (const auto& [_, edges] : State_->WorkerCoefEdges) {
            for (const auto& [_, edge] : edges) {
                if (!Touches(edge, worker)) {
                    continue;
                }
                ++count;
                if (!oldest || edge.UpdatedAt < oldest->UpdatedAt) {
                    oldest = &edge;
                }
            }
        }
        if (count <= Config_.MaxEdgesPerWorker) {
            return;
        }
        EraseEdge(oldest->From, oldest->To);
    }
}

void TWorkerCoefEstimator::EraseEdge(const std::string& from, const std::string& to)
{
    auto& edges = State_->WorkerCoefEdges;
    auto it = edges.find(from);
    if (it == edges.end()) {
        return;
    }
    it->second.erase(to);
    if (it->second.empty()) {
        edges.erase(it);
    }
}

void TWorkerCoefEstimator::Prune(const THashSet<std::string>& present, TInstant now)
{
    // Only workers with edges are worth remembering; the rest are 1 implicitly.
    THashSet<std::string> known;
    for (const auto& [from, edges] : State_->WorkerCoefEdges) {
        known.insert(from);
        for (const auto& [to, _] : edges) {
            known.insert(to);
        }
    }
    auto& lastSeen = State_->WorkerLastSeen;
    THashSet<std::string> forgotten;
    for (const auto& worker : known) {
        if (present.contains(worker)) {
            lastSeen[worker] = now;
            continue;
        }
        auto it = lastSeen.find(worker);
        if (it == lastSeen.end()) {
            // First noticed absent: start counting from now.
            lastSeen[worker] = now;
        } else if (it->second + Config_.Retention < now) {
            forgotten.insert(worker);
        }
    }
    for (auto& [from, edges] : State_->WorkerCoefEdges) {
        if (forgotten.contains(from)) {
            edges.clear();
            continue;
        }
        EraseNodesIf(edges, [&] (const auto& item) {
            return forgotten.contains(item.first);
        });
    }
    EraseNodesIf(State_->WorkerCoefEdges, [] (const auto& item) {
        return item.second.empty();
    });
    EraseNodesIf(lastSeen, [&] (const auto& item) {
        return !known.contains(item.first) || forgotten.contains(item.first);
    });
}

void TWorkerCoefEstimator::Solve()
{
    // Minimizes sum_e W_e (x_To - x_From - O_e)^2 + PriorWeight * sum_w x_w^2 by Gauss-Seidel
    // sweeps from the previous solution. The prior makes the minimum unique and pins the mean of
    // log(coef) in every connected component to zero, i.e. the geometric mean of coefficients to 1.
    // Workers and edges are visited in a fixed order so that the result does not depend on hash order.
    std::vector<std::string> unknowns;
    for (const auto& [from, edges] : State_->WorkerCoefEdges) {
        unknowns.push_back(from);
        for (const auto& [to, _] : edges) {
            unknowns.push_back(to);
        }
    }
    std::sort(unknowns.begin(), unknowns.end());
    unknowns.erase(std::unique(unknowns.begin(), unknowns.end()), unknowns.end());

    struct TNeighbour
    {
        std::string Worker;
        //! The observed x_this - x_Worker.
        double Obs;
        double Weight;
    };

    THashMap<std::string, std::vector<TNeighbour>> neighbours;
    for (const auto& [from, edges] : State_->WorkerCoefEdges) {
        for (const auto& [to, edge] : edges) {
            neighbours[from].push_back({to, -edge.Obs, edge.Weight});
            neighbours[to].push_back({from, edge.Obs, edge.Weight});
        }
    }
    for (auto& [_, list] : neighbours) {
        std::sort(list.begin(), list.end(), [] (const TNeighbour& lhs, const TNeighbour& rhs) {
            return lhs.Worker < rhs.Worker;
        });
    }

    // Connected components: summing the normal equations over a component shows that the exact
    // solution has zero mean log(coef) in each of them. Gauss-Seidel alone relaxes that common
    // mode only at the rate of the weak prior, so it is projected out after every sweep instead.
    THashMap<std::string, int> componentOf;
    std::vector<std::vector<std::string>> components;
    for (const auto& root : unknowns) {
        if (componentOf.contains(root)) {
            continue;
        }
        int component = std::ssize(components);
        components.emplace_back();
        std::vector<std::string> stack{root};
        componentOf[root] = component;
        while (!stack.empty()) {
            auto worker = std::move(stack.back());
            stack.pop_back();
            components[component].push_back(worker);
            if (auto* list = neighbours.FindPtr(worker)) {
                for (const auto& neighbour : *list) {
                    if (componentOf.try_emplace(neighbour.Worker, component).second) {
                        stack.push_back(neighbour.Worker);
                    }
                }
            }
        }
    }

    THashMap<std::string, double> x;
    for (const auto& worker : unknowns) {
        x[worker] = GetOrDefault(State_->WorkerLogCoefs, worker, 0.);
    }
    for (int sweep = 0; sweep < SolverMaxSweeps; ++sweep) {
        auto previous = x;
        for (const auto& worker : unknowns) {
            double numerator = 0.;
            double denominator = Config_.PriorWeight;
            if (auto* list = neighbours.FindPtr(worker)) {
                for (const auto& neighbour : *list) {
                    numerator += neighbour.Weight * (x[neighbour.Worker] + neighbour.Obs);
                    denominator += neighbour.Weight;
                }
            }
            x[worker] = numerator / denominator;
        }
        for (const auto& component : components) {
            double mean = 0.;
            for (const auto& worker : component) {
                mean += x[worker];
            }
            mean /= std::ssize(component);
            for (const auto& worker : component) {
                x[worker] -= mean;
            }
        }
        double maxDelta = 0.;
        for (const auto& worker : unknowns) {
            maxDelta = std::max(maxDelta, std::abs(x[worker] - previous[worker]));
        }
        if (maxDelta < SolverTolerance) {
            break;
        }
    }
    State_->WorkerLogCoefs = std::move(x);
}

int TWorkerCoefEstimator::GetEdgeCount() const
{
    int count = 0;
    for (const auto& [_, edges] : State_->WorkerCoefEdges) {
        count += std::ssize(edges);
    }
    return count;
}

double TWorkerCoefEstimator::GetCoef(const std::string& worker) const
{
    auto* logCoef = State_->WorkerLogCoefs.FindPtr(worker);
    if (!logCoef) {
        return 1.;
    }
    double limit = std::log(Config_.MaxRatio);
    return std::exp(std::clamp(*logCoef, -limit, limit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
