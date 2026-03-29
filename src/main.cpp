#include "libera/core/GlobalDacManager.hpp"
#include "libera/etherdream/EtherDreamManager.hpp"
#include "libera/helios/HeliosDeviceInfo.hpp"
#include "libera/helios/HeliosManager.hpp"
#include "libera/lasercubenet/LaserCubeNetManager.hpp"
#include "libera/lasercubeusb/LaserCubeUsbManager.hpp"

#include "output/V1LaproGraphOut.hpp"
#include "server/IDNLaproService.hpp"
#include "shared/DACHWInterface.hpp"
#include "stage/SockIDNServer.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// IDNBridge runtime overview:
// 1) OpenIDN receives UDP/IDN traffic and decodes LaPro chunks.
// 2) Decoded slices are pulled through DACHWInterface::getNextBuffer().
// 3) LiberaDacAdapter converts slices to LaserPoint and queues them.
// 4) Libera requests points via callback and streams them to hardware.
namespace {

using libera::core::LaserPoint;
using libera::core::PointFillRequest;

// Global shutdown flag set by SIGINT/SIGTERM and polled by worker loops.
std::atomic<bool> gStopRequested{false};

void onSignal(int) {
    // Keep signal handler minimal and async-safe: only set an atomic flag.
    gStopRequested.store(true, std::memory_order_relaxed);
}

// Runtime knobs for discovery and streaming behavior.
struct Options {
    std::uint32_t discoveryTimeoutMs = 5000;
    std::size_t maxDacs = 0;
    std::uint32_t sliceDurationUs = 15000;
    std::size_t maxQueuedPoints = 300000;
    std::uint32_t latencyMs = 50;
    std::uint32_t maxLatencyMs = 1500;
    bool autoLatency = true;
};

void printUsage(const char* exe) {
    std::cout << "Usage: " << exe << " [options]\n"
              << "  --discovery-timeout-ms <ms>    Libera discovery wait time (default 5000)\n"
              << "  --max-dacs <count>             Limit number of bridged DACs (default all)\n"
              << "  --slice-us <us>                Driver slice duration in microseconds (default 15000)\n"
              << "  --max-queue-points <count>     Max queued translated points per DAC (default 300000)\n"
              << "  --latency-ms <ms>              Target buffered latency in milliseconds (default 50)\n"
              << "  --max-latency-ms <ms>          Max auto latency in milliseconds (default 1500)\n"
              << "  --no-auto-latency              Disable automatic latency increase on underrun\n"
              << "  --help                         Show this message\n";
}

// Strict unsigned parser used for all numeric CLI options.
bool parseUnsigned(const std::string& text, std::uint64_t& value) {
    try {
        std::size_t parsed = 0;
        unsigned long long raw = std::stoull(text, &parsed, 10);
        if (parsed != text.size()) {
            return false;
        }
        value = static_cast<std::uint64_t>(raw);
        return true;
    } catch (...) {
        return false;
    }
}

enum class ParseResult {
    Ok,
    Help,
    Error
};

// Parse and clamp command-line options into a safe runtime config.
ParseResult parseOptions(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--help") {
            printUsage(argv[0]);
            return ParseResult::Help;
        }
        if (arg == "--no-auto-latency") {
            options.autoLatency = false;
            continue;
        }
        if (i + 1 >= argc) {
            std::cerr << "Missing value for " << arg << "\n";
            printUsage(argv[0]);
            return ParseResult::Error;
        }

        std::uint64_t raw = 0;
        if (!parseUnsigned(argv[i + 1], raw)) {
            std::cerr << "Invalid numeric value for " << arg << ": " << argv[i + 1] << "\n";
            return ParseResult::Error;
        }

        if (arg == "--discovery-timeout-ms") {
            options.discoveryTimeoutMs = static_cast<std::uint32_t>(std::min<std::uint64_t>(raw, std::numeric_limits<std::uint32_t>::max()));
        } else if (arg == "--max-dacs") {
            options.maxDacs = static_cast<std::size_t>(raw);
        } else if (arg == "--slice-us") {
            options.sliceDurationUs = static_cast<std::uint32_t>(std::max<std::uint64_t>(raw, 1000));
        } else if (arg == "--max-queue-points") {
            options.maxQueuedPoints = static_cast<std::size_t>(std::max<std::uint64_t>(raw, 1000));
        } else if (arg == "--latency-ms") {
            options.latencyMs = static_cast<std::uint32_t>(std::clamp<std::uint64_t>(raw, 0, 10000));
        } else if (arg == "--max-latency-ms") {
            options.maxLatencyMs = static_cast<std::uint32_t>(std::clamp<std::uint64_t>(raw, 0, 30000));
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            printUsage(argv[0]);
            return ParseResult::Error;
        }
        ++i;
    }
    if (options.maxLatencyMs < options.latencyMs) {
        options.maxLatencyMs = options.latencyMs;
    }
    return ParseResult::Ok;
}

// Adapter between OpenIDN service output (slices) and Libera's point stream API.
class LiberaDacAdapter final : public DACHWInterface {
public:
    // Snapshot struct for periodic diagnostics.
    struct StatsSnapshot {
        std::uint64_t receivedSlices = 0;
        std::uint64_t receivedPoints = 0;
        std::uint64_t receivedLitPoints = 0;
        std::uint64_t callbackCalls = 0;
        std::uint64_t callbackUnderrunEvents = 0;
        std::uint64_t callbackUnderrunPoints = 0;
        std::uint64_t emittedPoints = 0;
        std::uint64_t blankFillPoints = 0;
        std::uint64_t droppedPoints = 0;
        std::size_t queuedPoints = 0;
        std::uint32_t outputPointRate = 0;
        std::uint32_t observedInputPointRate = 0;
        std::uint32_t latencyMs = 0;
        std::size_t targetBufferedPoints = 0;
        bool buffering = false;
    };

    LiberaDacAdapter(std::shared_ptr<libera::core::LaserDevice> device,
                     std::string displayName,
                     unsigned maxPointRateValue,
                     std::size_t maxQueuedPoints,
                     std::uint32_t latencyMs,
                     std::uint32_t maxLatencyMs,
                     bool autoLatency)
    : device_(std::move(device))
    , displayName_(std::move(displayName))
    , maxPointRateValue_(maxPointRateValue > 0 ? maxPointRateValue : 100000u)
    , maxQueuedPoints_(std::max<std::size_t>(maxQueuedPoints, 1000))
    , latencyMs_(latencyMs)
    , maxLatencyMs_(std::max(maxLatencyMs, latencyMs))
    , autoLatency_(autoLatency) {
        // Start with a safe default and track incoming pps over time.
        const auto initialRate = std::min<std::uint32_t>(30000u, maxPointRateValue_);
        currentPointRate_ = std::max<std::uint32_t>(initialRate, 1000u);
        commandedInputPps_.store(currentPointRate_, std::memory_order_relaxed);
        buffering_.store(true, std::memory_order_relaxed);

        // Libera consumes points via callback (pull model), not push writes.
        device_->setPointRate(currentPointRate_);
        device_->setArmed(true);
        device_->setRequestPointsCallback(
            [this](const PointFillRequest& req, std::vector<LaserPoint>& out) {
                this->fillFromQueue(req, out);
            });
    }

    ~LiberaDacAdapter() override {
        if (device_) {
            device_->setRequestPointsCallback({});
        }
    }

    int writeFrame(const TimeSlice& slice, double durationUs) override {
        // OpenIDN ingress: each slice is decoded point data for this endpoint.
        const auto bpp = bytesPerPoint();
        if (bpp == 0 || slice.dataChunk.size() < bpp) {
            return 0;
        }

        const std::size_t pointCount = slice.dataChunk.size() / bpp;
        const unsigned commandedPointRate = inferCommandedPointRate(slice, pointCount, durationUs);
        receivedSlices_.fetch_add(1, std::memory_order_relaxed);
        receivedPoints_.fetch_add(pointCount, std::memory_order_relaxed);
        const auto* points = reinterpret_cast<const ISPDB25Point*>(slice.dataChunk.data());

        std::vector<LaserPoint> translated;
        translated.reserve(pointCount);
        for (std::size_t i = 0; i < pointCount; ++i) {
            translated.push_back(toLaserPoint(points[i]));
        }
        const std::size_t litCount = std::count_if(
            translated.begin(), translated.end(),
            [](const LaserPoint& p) {
                return p.i > 0.0f && (p.r > 0.0f || p.g > 0.0f || p.b > 0.0f);
            });
        receivedLitPoints_.fetch_add(litCount, std::memory_order_relaxed);

        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            const std::size_t projected = pendingPoints_.size() + translated.size();
            if (projected > maxQueuedPoints_) {
                const std::size_t overflow = projected - maxQueuedPoints_;
                const std::size_t dropCount = std::min<std::size_t>(overflow, pendingPoints_.size());
                for (std::size_t i = 0; i < dropCount; ++i) {
                    pendingPoints_.pop_front();
                }
                droppedPoints_.fetch_add(dropCount, std::memory_order_relaxed);
            }
            for (auto& p : translated) {
                pendingPoints_.push_back(p);
            }
        }

        maybeUpdatePointRate(commandedPointRate);

        // Keep adapter contract consistent with existing OpenIDN hardware adapters.
        return 0;
    }

    SliceType convertPoints(const std::vector<ISPDB25Point>& points) override {
        // Serialization helper used by the inherited OpenIDN adapter pipeline.
        SliceType bytes(points.size() * sizeof(ISPDB25Point));
        if (!bytes.empty()) {
            std::memcpy(bytes.data(), points.data(), bytes.size());
        }
        return bytes;
    }

    unsigned bytesPerPoint() override {
        return static_cast<unsigned>(sizeof(ISPDB25Point));
    }

    unsigned maxBytesPerTransmission() override {
        return static_cast<unsigned>(4096 * sizeof(ISPDB25Point));
    }

    unsigned maxPointrate() override {
        return maxPointRateValue_;
    }

    void setMaxPointrate(unsigned rate) override {
        maxPointRateValue_ = std::max<unsigned>(rate, 1000);
        if (currentPointRate_ > maxPointRateValue_) {
            currentPointRate_ = maxPointRateValue_;
            device_->setPointRate(currentPointRate_);
        }
    }

    void getName(char *nameBufferPtr, unsigned nameBufferSize) override {
        if (nameBufferPtr == nullptr || nameBufferSize == 0) {
            return;
        }
        std::snprintf(nameBufferPtr, nameBufferSize, "%s", displayName_.c_str());
    }

    StatsSnapshot getStatsSnapshot() {
        StatsSnapshot snapshot;
        snapshot.receivedSlices = receivedSlices_.load(std::memory_order_relaxed);
        snapshot.receivedPoints = receivedPoints_.load(std::memory_order_relaxed);
        snapshot.receivedLitPoints = receivedLitPoints_.load(std::memory_order_relaxed);
        snapshot.callbackCalls = callbackCalls_.load(std::memory_order_relaxed);
        snapshot.callbackUnderrunEvents = callbackUnderrunEvents_.load(std::memory_order_relaxed);
        snapshot.callbackUnderrunPoints = callbackUnderrunPoints_.load(std::memory_order_relaxed);
        snapshot.emittedPoints = emittedPoints_.load(std::memory_order_relaxed);
        snapshot.blankFillPoints = blankFillPoints_.load(std::memory_order_relaxed);
        snapshot.droppedPoints = droppedPoints_.load(std::memory_order_relaxed);
        snapshot.outputPointRate = currentPointRate_;
        snapshot.observedInputPointRate = commandedInputPps_.load(std::memory_order_relaxed);
        snapshot.latencyMs = latencyMs_.load(std::memory_order_relaxed);
        snapshot.targetBufferedPoints = targetBufferedPoints();
        snapshot.buffering = buffering_.load(std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            snapshot.queuedPoints = pendingPoints_.size();
        }
        return snapshot;
    }

private:
    static unsigned inferCommandedPointRate(const TimeSlice& slice,
                                            std::size_t pointCount,
                                            double durationUs) {
        // Infer pps from point count and slice duration when possible.
        if (pointCount == 0) {
            return 0;
        }

        double effectiveDurationUs = durationUs;
        if (effectiveDurationUs <= 0.0) {
            effectiveDurationUs = static_cast<double>(slice.durationUs);
        }
        if (effectiveDurationUs <= 0.0) {
            return 0;
        }

        const double inferredRate =
            (1000000.0 * static_cast<double>(pointCount)) / effectiveDurationUs;
        if (!std::isfinite(inferredRate) || inferredRate <= 0.0) {
            return 0;
        }
        return static_cast<unsigned>(std::llround(inferredRate));
    }

    std::size_t targetBufferedPoints() const {
        // Convert latency target (ms) into queue depth in points.
        const double latencyMs = static_cast<double>(latencyMs_.load(std::memory_order_relaxed));
        const double rawPoints = (static_cast<double>(currentPointRate_) * latencyMs) / 1000.0;
        const std::size_t minPoints = 1000;
        const std::size_t maxPoints = std::max<std::size_t>(minPoints, maxQueuedPoints_ - 1);
        return std::clamp<std::size_t>(
            static_cast<std::size_t>(std::llround(rawPoints)),
            minPoints,
            maxPoints);
    }

    static float unitFromU16(std::uint16_t value) {
        return static_cast<float>(value) / 65535.0f;
    }

    static float signedUnitFromU16(std::uint16_t value) {
        return unitFromU16(value) * 2.0f - 1.0f;
    }

    static LaserPoint toLaserPoint(const ISPDB25Point& in) {
        // Convert OpenIDN's 16-bit fixed-range point fields to Libera floats.
        LaserPoint out{};
        out.x = std::clamp(signedUnitFromU16(in.x), -1.0f, 1.0f);
        out.y = std::clamp(signedUnitFromU16(in.y), -1.0f, 1.0f);

        out.r = std::clamp(unitFromU16(in.r), 0.0f, 1.0f);
        out.g = std::clamp(unitFromU16(in.g), 0.0f, 1.0f);
        out.b = std::clamp(unitFromU16(in.b), 0.0f, 1.0f);
        out.i = std::clamp(unitFromU16(in.intensity), 0.0f, 1.0f);

        // OpenIDN's decoder commonly leaves shutter at 0 for bridged point streams.
        // Treat color/intensity as authoritative so we don't blank valid frames.
        const float rgbMax = std::max(out.r, std::max(out.g, out.b));
        if (out.i <= 0.0f && rgbMax > 0.0f) {
            out.i = rgbMax;
        }

        if (out.i <= 0.0f) {
            out.r = 0.0f;
            out.g = 0.0f;
            out.b = 0.0f;
            out.i = 0.0f;
        }

        out.u1 = std::clamp(unitFromU16(in.u1), 0.0f, 1.0f);
        out.u2 = std::clamp(unitFromU16(in.u2), 0.0f, 1.0f);
        return out;
    }

    void fillFromQueue(const PointFillRequest& req, std::vector<LaserPoint>& out) {
        // Libera output thread pulls points from us here.
        if (req.maximumPointsRequired == 0) {
            return;
        }

        callbackCalls_.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(queueMutex_);
        const std::size_t availableAtStart = pendingPoints_.size();
        const std::size_t targetPoints = targetBufferedPoints();
        if (buffering_.load(std::memory_order_relaxed) && availableAtStart < targetPoints) {
            // During warmup/rebuffer, output blanks until queue is sufficiently primed.
            const std::size_t missing = req.minimumPointsRequired;
            callbackUnderrunEvents_.fetch_add(1, std::memory_order_relaxed);
            callbackUnderrunPoints_.fetch_add(missing, std::memory_order_relaxed);
            logUnderrunIfDue(req, availableAtStart, missing, targetPoints, true);
            appendFallbackPoints(out, missing);
            blankFillPoints_.fetch_add(missing, std::memory_order_relaxed);
            return;
        }
        buffering_.store(false, std::memory_order_relaxed);

        const std::size_t toWrite = std::min<std::size_t>(req.maximumPointsRequired, pendingPoints_.size());
        for (std::size_t i = 0; i < toWrite; ++i) {
            out.push_back(pendingPoints_.front());
            pendingPoints_.pop_front();
        }
        emittedPoints_.fetch_add(toWrite, std::memory_order_relaxed);
        if (toWrite > 0) {
            lastOutputPoint_ = out.back();
            haveLastOutputPoint_ = true;
        }

        if (out.size() < req.minimumPointsRequired) {
            // Underrun: backfill with blank points to keep scanner timing continuous.
            const std::size_t blankCount = req.minimumPointsRequired - out.size();
            callbackUnderrunEvents_.fetch_add(1, std::memory_order_relaxed);
            callbackUnderrunPoints_.fetch_add(blankCount, std::memory_order_relaxed);
            maybeIncreaseLatencyOnUnderrun(blankCount, req.minimumPointsRequired, targetPoints);
            logUnderrunIfDue(req, availableAtStart, blankCount, targetPoints, false);
            appendFallbackPoints(out, blankCount);
            blankFillPoints_.fetch_add(blankCount, std::memory_order_relaxed);
            buffering_.store(true, std::memory_order_relaxed);
        }
    }

    void appendFallbackPoints(std::vector<LaserPoint>& out, std::size_t count) {
        // Hold last XY position but force intensity/color to zero.
        LaserPoint fallback{};
        if (haveLastOutputPoint_) {
            fallback = lastOutputPoint_;
            fallback.r = 0.0f;
            fallback.g = 0.0f;
            fallback.b = 0.0f;
            fallback.i = 0.0f;
        }
        out.insert(out.end(), count, fallback);
    }

    void maybeIncreaseLatencyOnUnderrun(std::size_t missingPoints,
                                        std::size_t minimumPointsRequired,
                                        std::size_t targetPoints) {
        // Simple adaptive latency control to reduce repeated underruns.
        if (!autoLatency_) {
            return;
        }

        if (minimumPointsRequired == 0) {
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        if ((now - lastLatencyUnderrunSample_) > std::chrono::milliseconds(2000)) {
            latencyUnderrunAccumulator_ = 0;
        }
        lastLatencyUnderrunSample_ = now;
        latencyUnderrunAccumulator_ += missingPoints;

        if ((now - lastLatencyIncrease_) < std::chrono::milliseconds(3000)) {
            return;
        }

        const std::size_t threshold = std::max<std::size_t>(
            minimumPointsRequired / 2,
            std::max<std::size_t>(100, targetPoints / 3));
        if (latencyUnderrunAccumulator_ < threshold) {
            return;
        }

        const std::uint32_t before = latencyMs_.load(std::memory_order_relaxed);
        if (before >= maxLatencyMs_) {
            latencyUnderrunAccumulator_ = 0;
            return;
        }

        const std::uint32_t step = std::max<std::uint32_t>(10, before / 10);
        const std::uint32_t after = std::min<std::uint32_t>(maxLatencyMs_, before + step);
        latencyMs_.store(after, std::memory_order_relaxed);
        lastLatencyIncrease_ = now;
        latencyUnderrunAccumulator_ = 0;

        if (after != before) {
            std::cout << "[bridge:" << displayName_ << "] auto-latency "
                      << before << "ms -> " << after << "ms" << std::endl;
        }
    }

    void logUnderrunIfDue(const PointFillRequest& req,
                          std::size_t available,
                          std::size_t missing,
                          std::size_t targetPoints,
                          bool bufferingHold) {
        // Throttle underrun logs to avoid overwhelming stdout.
        const auto now = std::chrono::steady_clock::now();
        if ((now - lastUnderrunLog_) < std::chrono::milliseconds(1000)) {
            return;
        }
        lastUnderrunLog_ = now;

        std::cout << "[bridge:" << displayName_ << "] underrun"
                  << " need_min=" << req.minimumPointsRequired
                  << " need_max=" << req.maximumPointsRequired
                  << " available=" << available
                  << " missing=" << missing
                  << " target=" << targetPoints
                  << " queue_now=" << pendingPoints_.size()
                  << " buffering=" << (bufferingHold ? 1 : 0)
                  << std::endl;
    }

    void maybeUpdatePointRate(unsigned commandedPointRate) {
        // Keep Libera output pps aligned with incoming IDN timing metadata.
        if (commandedPointRate == 0) {
            return;
        }
        const std::uint32_t proposed = std::clamp<std::uint32_t>(commandedPointRate, 1000u, maxPointRateValue_);
        commandedInputPps_.store(proposed, std::memory_order_relaxed);
        if (proposed == currentPointRate_) {
            return;
        }
        device_->setPointRate(proposed);
        currentPointRate_ = proposed;
    }

    // State shared by OpenIDN ingress thread and Libera callback thread.
    std::shared_ptr<libera::core::LaserDevice> device_;
    std::string displayName_;
    unsigned maxPointRateValue_;
    std::size_t maxQueuedPoints_;
    std::uint32_t currentPointRate_ = 30000;
    std::atomic<std::uint32_t> latencyMs_{300};
    std::uint32_t maxLatencyMs_ = 1500;
    bool autoLatency_ = true;
    std::atomic<std::uint32_t> commandedInputPps_{0};
    std::chrono::steady_clock::time_point lastLatencyIncrease_{};
    std::chrono::steady_clock::time_point lastLatencyUnderrunSample_{};
    std::size_t latencyUnderrunAccumulator_ = 0;
    std::mutex queueMutex_;
    std::deque<LaserPoint> pendingPoints_;
    LaserPoint lastOutputPoint_{};
    bool haveLastOutputPoint_ = false;
    std::atomic<bool> buffering_{true};
    std::chrono::steady_clock::time_point lastUnderrunLog_{};
    std::atomic<std::uint64_t> receivedSlices_{0};
    std::atomic<std::uint64_t> receivedPoints_{0};
    std::atomic<std::uint64_t> receivedLitPoints_{0};
    std::atomic<std::uint64_t> callbackCalls_{0};
    std::atomic<std::uint64_t> callbackUnderrunEvents_{0};
    std::atomic<std::uint64_t> callbackUnderrunPoints_{0};
    std::atomic<std::uint64_t> emittedPoints_{0};
    std::atomic<std::uint64_t> blankFillPoints_{0};
    std::atomic<std::uint64_t> droppedPoints_{0};
};

// One logical bridge endpoint:
// - one Libera device
// - one OpenIDN service ID/name
// - one local worker thread that moves OpenIDN slices into the adapter queue
class IdnBridgeEndpoint {
public:
    IdnBridgeEndpoint(std::shared_ptr<libera::core::LaserDevice> device,
                      std::string dacLabel,
                      std::string dacId,
                      std::string dacType,
                      unsigned maxPps,
                      std::uint32_t sliceDurationUs,
                      std::size_t maxQueuedPoints,
                      std::uint32_t latencyMs,
                      std::uint32_t maxLatencyMs,
                      bool autoLatency,
                      std::uint8_t serviceId)
    : label_(std::move(dacLabel))
    , id_(std::move(dacId))
    , type_(std::move(dacType))
    , serviceId_(serviceId)
    , sliceDurationUs_(sliceDurationUs)
    , adapter_(std::make_shared<LiberaDacAdapter>(
          std::move(device), label_, maxPps, maxQueuedPoints, latencyMs, maxLatencyMs, autoLatency))
    , output_(std::make_unique<V1LaproGraphicOutput>(adapter_)) {
        // Service name is what IDN clients will see in service map responses.
        const std::string bridgedServiceName = std::string("IDNBridge ") + label_;
        std::vector<char> serviceName(bridgedServiceName.begin(), bridgedServiceName.end());
        serviceName.push_back('\0');
        const bool isDefault = (serviceId_ == 1);
        service_ = std::make_unique<IDNLaproService>(serviceId_, serviceName.data(), isDefault, output_.get());
    }

    ~IdnBridgeEndpoint() {
        stop();
    }

    void start() {
        // Start the per-endpoint OpenIDN->Libera transfer loop.
        if (running_.exchange(true)) {
            return;
        }

        driverThread_ = std::thread([this] { this->driverLoop(); });
    }

    void stop() {
        // Idempotent shutdown for clean tear-down paths.
        if (!running_.exchange(false)) {
            return;
        }

        if (driverThread_.joinable()) {
            driverThread_.join();
        }
    }

    void linkService(LLNode<ServiceNode>** firstService) {
        // Register service in OpenIDN's linked list used by SockIDNServer.
        if (!service_ || !firstService) {
            return;
        }
        service_->linkinLast(firstService);
    }

    const std::string& label() const { return label_; }
    const std::string& id() const { return id_; }
    const std::string& type() const { return type_; }
    std::uint8_t serviceId() const { return serviceId_; }
    void logStatsIfDue() {
        // Emit rolling per-second deltas to make throughput regressions obvious.
        using namespace std::chrono_literals;
        const auto now = std::chrono::steady_clock::now();
        if ((now - lastStatsLog_) < 1s) {
            return;
        }
        lastStatsLog_ = now;

        const auto stats = adapter_->getStatsSnapshot();
        const auto deltaRxPts = stats.receivedPoints - lastStats_.receivedPoints;
        const auto deltaRxLitPts = stats.receivedLitPoints - lastStats_.receivedLitPoints;
        const auto deltaRxSlices = stats.receivedSlices - lastStats_.receivedSlices;
        const auto deltaCb = stats.callbackCalls - lastStats_.callbackCalls;
        const auto deltaUnderrunCb = stats.callbackUnderrunEvents - lastStats_.callbackUnderrunEvents;
        const auto deltaUnderrunPts = stats.callbackUnderrunPoints - lastStats_.callbackUnderrunPoints;
        const auto deltaOutPts = stats.emittedPoints - lastStats_.emittedPoints;
        const auto deltaBlank = stats.blankFillPoints - lastStats_.blankFillPoints;
        const auto deltaDropped = stats.droppedPoints - lastStats_.droppedPoints;

        std::cout << "[bridge:" << label_ << "]"
                  << " rx_slices/s=" << deltaRxSlices
                  << " rx_pts/s=" << deltaRxPts
                  << " rx_lit_pts/s=" << deltaRxLitPts
                  << " cb/s=" << deltaCb
                  << " und_cb/s=" << deltaUnderrunCb
                  << " und_pts/s=" << deltaUnderrunPts
                  << " out_pts/s=" << deltaOutPts
                  << " pps=" << stats.outputPointRate
                  << " in_pps=" << stats.observedInputPointRate
                  << " lat_ms=" << stats.latencyMs
                  << " lat_pts=" << stats.targetBufferedPoints
                  << " buffering=" << (stats.buffering ? 1 : 0)
                  << " queue=" << stats.queuedPoints
                  << " blank/s=" << deltaBlank
                  << " drop/s=" << deltaDropped
                  << std::endl;

        lastStats_ = stats;
    }

private:
    void driverLoop() {
        using namespace std::chrono_literals;

        TransformEnv tfEnv;
        tfEnv.usPerSlice = static_cast<double>(sliceDurationUs_);
        tfEnv.currentSliceTime = tfEnv.usPerSlice;

        unsigned driverMode = DRIVER_INACTIVE;
        auto currentBuffer = std::make_shared<SliceBuf>();

        while (running_.load(std::memory_order_relaxed)) {
            // Pull newly decoded slices from OpenIDN pipeline.
            auto nextBuffer = adapter_->getNextBuffer(tfEnv, driverMode);
            if (nextBuffer && !nextBuffer->empty()) {
                currentBuffer = nextBuffer;
            }

            if (!currentBuffer || currentBuffer->empty()) {
                std::this_thread::sleep_for(1ms);
                continue;
            }

            const std::size_t iterationCount = currentBuffer->size();
            for (std::size_t i = 0; i < iterationCount && running_.load(std::memory_order_relaxed); ++i) {
                auto nextSlice = currentBuffer->front();
                currentBuffer->pop_front();

                if (!nextSlice) {
                    continue;
                }

                // Hand the slice to adapter; adapter translates + enqueues points.
                adapter_->writeFrame(*nextSlice, static_cast<double>(nextSlice->durationUs));

                if (driverMode == DRIVER_FRAMEMODE) {
                    // Frame mode replays the last frame continuously until replaced.
                    currentBuffer->push_back(nextSlice);
                }
            }
        }
    }

    std::string label_;
    std::string id_;
    std::string type_;
    std::uint8_t serviceId_;
    std::uint32_t sliceDurationUs_;

    std::shared_ptr<LiberaDacAdapter> adapter_;
    std::unique_ptr<V1LaproGraphicOutput> output_;
    std::unique_ptr<IDNLaproService> service_;
    std::thread driverThread_;
    std::atomic<bool> running_{false};
    std::chrono::steady_clock::time_point lastStatsLog_{};
    LiberaDacAdapter::StatsSnapshot lastStats_{};
};

std::vector<std::unique_ptr<libera::core::DacInfo>> discoverDacs(libera::core::GlobalDacManager& manager,
                                                                  std::uint32_t timeoutMs) {
    // Poll discovery until at least one DAC appears, timeout, or shutdown.
    using namespace std::chrono_literals;
    auto started = std::chrono::steady_clock::now();
    std::vector<std::unique_ptr<libera::core::DacInfo>> discovered;

    while (!gStopRequested.load(std::memory_order_relaxed)) {
        discovered = manager.discoverAll();
        if (!discovered.empty()) {
            return discovered;
        }

        if (timeoutMs > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started);
            if (elapsed.count() >= timeoutMs) {
                return discovered;
            }
        }

        std::this_thread::sleep_for(200ms);
    }

    return discovered;
}

bool shouldBridgeDac(const libera::core::DacInfo& info, std::string& reason) {
    // Skip already-IDN Helios network DACs. Keep USB Helios and other non-IDN DAC types.
    if (info.type() == "helios") {
        const auto* heliosInfo = dynamic_cast<const libera::helios::HeliosDeviceInfo*>(&info);
        if (heliosInfo != nullptr && !heliosInfo->isUsbDevice()) {
            reason = "already an IDN network Helios DAC";
            return false;
        }
    }

    reason.clear();
    return true;
}

} // namespace

int main(int argc, char** argv) {
    // Phase 1: parse configuration.
    Options options;
    const ParseResult parseResult = parseOptions(argc, argv, options);
    if (parseResult == ParseResult::Help) {
        return 0;
    }
    if (parseResult != ParseResult::Ok) {
        return 1;
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    // Phase 2: discover and connect DACs via Libera.
    libera::core::GlobalDacManager manager;
    auto discovered = discoverDacs(manager, options.discoveryTimeoutMs);
    if (discovered.empty()) {
        std::cerr << "No DACs discovered via Libera.\n";
        return 1;
    }

    std::cout << "Discovered " << discovered.size() << " DAC(s) via Libera.\n";

    LLNode<ServiceNode>* firstService = nullptr;
    std::vector<std::unique_ptr<IdnBridgeEndpoint>> endpoints;
    endpoints.reserve(discovered.size());

    std::size_t startedEndpoints = 0;
    for (const auto& info : discovered) {
        if (!info) {
            continue;
        }

        std::string skipReason;
        if (!shouldBridgeDac(*info, skipReason)) {
            std::cout << "Skipping " << info->labelValue()
                      << " [" << info->type() << ":" << info->idValue() << "]"
                      << " (" << skipReason << ")\n";
            continue;
        }

        if (options.maxDacs > 0 && startedEndpoints >= options.maxDacs) {
            break;
        }

        auto device = manager.getAndConnectToDac(*info);
        if (!device) {
            std::cerr << "Skipping " << info->labelValue() << " (connect failed)\n";
            continue;
        }

        const auto serviceIdRaw = startedEndpoints + 1;
        if (serviceIdRaw > 255) {
            std::cerr << "Service ID range exceeded. Stopping at " << startedEndpoints << " DAC(s).\n";
            break;
        }

        auto endpoint = std::make_unique<IdnBridgeEndpoint>(
            device,
            info->labelValue(),
            info->idValue(),
            info->type(),
            info->maxPointRate(),
            options.sliceDurationUs,
            options.maxQueuedPoints,
            options.latencyMs,
            options.maxLatencyMs,
            options.autoLatency,
            static_cast<std::uint8_t>(serviceIdRaw));

        // Attach endpoint service to OpenIDN service chain and start worker.
        endpoint->linkService(&firstService);
        endpoint->start();
        std::cout << "Bridged " << endpoint->label()
                  << " [" << endpoint->type() << ":" << endpoint->id() << "]"
                  << " -> IDN service " << static_cast<unsigned>(endpoint->serviceId()) << "\n";

        endpoints.push_back(std::move(endpoint));
        ++startedEndpoints;
    }

    if (endpoints.empty()) {
        std::cerr << "No bridge endpoints started.\n";
        manager.close();
        return 1;
    }

    // Phase 3: start OpenIDN UDP server thread.
    SockIDNServer server(firstService);
    std::thread serverThread([&server] { server.networkThreadFunc(); });

    std::cout << "Bridge running. Press Ctrl+C to stop.\n";

    while (!gStopRequested.load(std::memory_order_relaxed)) {
        // Keep main thread lightweight: only metrics + shutdown polling.
        for (auto& endpoint : endpoints) {
            endpoint->logStatsIfDue();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Phase 4: coordinated shutdown (server -> endpoints -> manager).
    server.stopServer();
    if (serverThread.joinable()) {
        serverThread.join();
    }

    for (auto& endpoint : endpoints) {
        endpoint->stop();
    }
    endpoints.clear();

    manager.close();
    std::cout << "Bridge stopped.\n";
    return 0;
}
