#include "filesystem/posixFileSystem.hpp"
#include "kv/kv.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using namespace aws::gg;
using namespace aws::gg::kv;
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

static auto open_kv(const std::string &path) {
    return KV::openOrCreate(KVOptions{
        .full_corruption_check_on_open = true,
        .filesystem_implementation = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(path)),
        .logger = logger,
        .identifier = "test-kv-map",
        .compact_after = 0,
    });
}

static auto open_kv_manual_compaction(const std::string &path) {
    return KV::openOrCreate(KVOptions{
        .full_corruption_check_on_open = true,
        .filesystem_implementation = std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(path)),
        .logger = logger,
        .identifier = "test-kv-map",
        .compact_after = -1,
    });
}

SCENARIO("I cannot create a KV map with invalid inputs", "kv") {
    auto kv_or = KV::openOrCreate(KVOptions{
        .filesystem_implementation{},
        .identifier{"test-kv-map"},
    });
    REQUIRE(!kv_or);
    REQUIRE(kv_or.err().code == KVErrorCodes::InvalidArguments);

    auto temp_dir = TempDir();
    kv_or = KV::openOrCreate(KVOptions{
        .filesystem_implementation{std::make_shared<SpyFileSystem>(std::make_shared<PosixFileSystem>(temp_dir.path()))},
        .identifier{}});
    REQUIRE(!kv_or);
    REQUIRE(kv_or.err().code == KVErrorCodes::InvalidArguments);
}

SCENARIO("I cannot put invalid inputs to the map", "[kv]") {
    auto temp_dir = TempDir();
    auto kv_or = open_kv(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    {
        auto e = kv->put("", BorrowedSlice{""});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("empty") != std::string::npos);
    }

    {
        std::string key{};
        key.resize(UINT32_MAX);
        auto e = kv->put(key, BorrowedSlice{""});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("Key length") != std::string::npos);
    }

    {
        std::string key{};
        auto e = kv->put("a", BorrowedSlice{key.data(), UINT32_MAX});
        REQUIRE(e.code == KVErrorCodes::InvalidArguments);
        REQUIRE(e.msg.find("Value length") != std::string::npos);
    }
}

SCENARIO("I create a KV map with manual compaction", "[kv]") {
    auto temp_dir = TempDir();
    auto kv_or = open_kv_manual_compaction(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    std::vector<std::string> keys{};
    auto key_gen = random(1, 512);
    for (int i = 0; i < 20; i++) {
        keys.emplace_back(key_gen.get());
        key_gen.next();
    }
    const std::string &value = GENERATE(take(1, random(1, 1 * 1024 * 1024)));

    // Put duplicated key-values so that we would benefit from compaction
    for (int i = 0; i < 10; i++) {
        for (const auto &k : keys) {
            auto e = kv->put(k, BorrowedSlice{value});
            REQUIRE(e.code == KVErrorCodes::NoError);
        }
    }

    auto size_now = kv->currentSizeBytes();
    auto e = kv->compact();
    REQUIRE(e.code == KVErrorCodes::NoError);
    // Ensure we got smaller
    REQUIRE(kv->currentSizeBytes() < size_now);
}

SCENARIO("I open a KV map from a shadow file", "[kv]") {
    auto temp_dir = TempDir();
    std::filesystem::create_directories(temp_dir.path());

    // map1 is a valid KV map which is truncated to 5K which makes the last value unreadable.
    // This will verify that we're still able to load the file and read as much as is uncorrupted.
    std::filesystem::copy_file(std::filesystem::path(__FILE__).parent_path() / "test_data" / "kv" / "map1.kv",
                               // identifier is test-kv-map, so the shadow file name is test-kv-maps
                               temp_dir.path() / "test-kv-maps");

    auto kv_or = open_kv(temp_dir.path());
    REQUIRE(kv_or);
    auto kv = std::move(kv_or.val());

    auto keys_or = kv->listKeys();
    REQUIRE(keys_or);
    auto keys = keys_or.val();
    REQUIRE(keys.size() == 1);
    REQUIRE(keys.front() == "a"s);
}

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        auto temp_dir = TempDir();
        auto kv_or = open_kv(temp_dir.path());
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        auto keys_or = kv->listKeys();
        REQUIRE(keys_or);
        REQUIRE(keys_or.val().empty());
        auto e = kv->put("key", BorrowedSlice{"value"});
        REQUIRE(e.code == KVErrorCodes::NoError);

        keys_or = kv->listKeys();
        REQUIRE(keys_or);
        REQUIRE(!keys_or.val().empty());
        REQUIRE(keys_or.val()[0] == "key"s);

        e = kv->compact();
        REQUIRE(e.code == KVErrorCodes::NoError);

        const std::string &key = GENERATE(take(10, random(1, 512)));
        const std::string &value = GENERATE(take(1, random(1, 1 * 1024 * 1024)));
        const std::string_view new_value = "new value"sv;

        WHEN("I add a value") {
            e = kv->put(key, BorrowedSlice{value});
            REQUIRE(e.code == KVErrorCodes::NoError);

            THEN("I can get the value back") {
                auto v_or = kv->get(key);
                REQUIRE(v_or);
                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == value);

                AND_WHEN("I update the value") {
                    e = kv->put(key, BorrowedSlice{new_value.data(), static_cast<uint32_t>(new_value.size())});
                    REQUIRE(e.code == KVErrorCodes::NoError);
                    THEN("I get the new value back") {
                        v_or = kv->get(key);
                        REQUIRE(v_or);
                        REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == new_value);

                        e = kv->put(key, BorrowedSlice{value});
                        REQUIRE(e.code == KVErrorCodes::NoError);

                        AND_WHEN("I close the KV and open it again") {
                            kv.reset();

                            kv_or = open_kv(temp_dir.path());
                            REQUIRE(kv_or);

                            kv = std::move(kv_or.val());

                            THEN("I can get the value back") {
                                v_or = kv->get(key);
                                REQUIRE(v_or);
                                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == value);

                                AND_WHEN("I remove the key") {
                                    e = kv->remove(key);
                                    REQUIRE(e.code == KVErrorCodes::NoError);

                                    THEN("I can't get the value back") {
                                        v_or = kv->get(key);
                                        REQUIRE(!v_or);
                                        REQUIRE(v_or.err().code == KVErrorCodes::KeyNotFound);

                                        e = kv->remove("non-existent-key");
                                        REQUIRE(e.code == KVErrorCodes::KeyNotFound);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}