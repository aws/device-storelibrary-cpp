#include "filesystem/posixFileSystem.hpp"
#include "stream/fileStream.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string>
#include <string_view>

using namespace aws::gg;
using namespace std::string_view_literals;
using namespace std::string_literals;

class Logger : public logging::Logger {
    void log(logging::LogLevel level, const std::string &msg) const override {
        switch (level) {
        case logging::LogLevel::Disabled:
            break;

        case logging::LogLevel::Trace:
            break;
        case logging::LogLevel::Debug:
            break;
        case logging::LogLevel::Info: {
            INFO(msg);
            break;
        }
        case logging::LogLevel::Warning: {
            WARN(msg);
            break;
        }
        case logging::LogLevel::Error: {
            WARN(msg);
            break;
        }
        }
    }
};

static const auto logger = std::make_shared<Logger>();

static auto open_stream(std::shared_ptr<FileSystemInterface> fs) {
    return FileStream::openOrCreate(StreamOptions{
        .minimum_segment_size_bytes = 1024 * 1024,
        .maximum_size_bytes = 10 * 1024 * 1024,
        .file_implementation = fs,
        .logger = logger,
        .kv_options =
            kv::KVOptions{
                .filesystem_implementation = fs,
                .logger = logger,
                .identifier = "m",
                .compact_after = 1 * 1024,
            },
    });
}

SCENARIO("I can create a stream", "[stream]") {
    auto temp_dir = TempDir();
    auto fs = std::make_shared<PosixFileSystem>(temp_dir.path());
    auto stream_or = open_stream(fs);
    REQUIRE(stream_or);
    auto stream = std::move(stream_or.val());

    const std::string &value = GENERATE(take(5, random(1, 1 * 1024 * 1024, 0, static_cast<char>(255))));

    WHEN("I append values") {
        auto seq_or = stream->append(BorrowedSlice{value});
        REQUIRE(seq_or);
        seq_or = stream->append(BorrowedSlice{value});
        REQUIRE(seq_or);

        auto it = stream->openOrCreateIterator("ita", IteratorOptions{});
        REQUIRE(it.sequence_number == 0);
        auto v_or = *it;
        REQUIRE(v_or);
        REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);

        ++it;
        REQUIRE(it.sequence_number == 1);
        v_or = *it;
        REQUIRE(v_or);
        v_or.val().checkpoint();

        it = stream->openOrCreateIterator("ita", IteratorOptions{});
        REQUIRE(it.sequence_number == 1);

        ++it;
        REQUIRE(it.sequence_number == 2);
        v_or = *it;
        REQUIRE(!v_or);
        REQUIRE(v_or.err().code == StreamErrorCode::RecordNotFound);

        stream->deleteIterator("ita");

        AND_WHEN("I close and reopen the stream") {
            stream = nullptr;

            stream_or = open_stream(fs);
            REQUIRE(stream_or);
            stream = std::move(stream_or.val());

            it = stream->openOrCreateIterator("ita", IteratorOptions{});
            REQUIRE(it.sequence_number == 0);
            v_or = *it;
            REQUIRE(v_or);
            REQUIRE(std::string_view{v_or.val().data.char_data(), v_or.val().data.size()} == value);
        }
    }
}