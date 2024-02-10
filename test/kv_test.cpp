#include "filesystem/posixFileSystem.hpp"
#include "kv/kv.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string>
#include <string_view>

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

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        auto temp_dir = TempDir();
        auto kv_or = KV::openOrCreate(KVOptions{
            .filesystem_implementation = std::make_shared<PosixFileSystem>(temp_dir.path()),
            .logger = logger,
            .identifier = "test-kv-map",
            .compact_after = 0,
        });
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
                    e = kv->put(key, BorrowedSlice{new_value.data(), new_value.size()});
                    REQUIRE(e.code == KVErrorCodes::NoError);
                    THEN("I get the new value back") {
                        v_or = kv->get(key);
                        REQUIRE(v_or);
                        REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == new_value);

                        e = kv->put(key, BorrowedSlice{value});
                        REQUIRE(e.code == KVErrorCodes::NoError);

                        AND_WHEN("I close the KV and open it again") {
                            kv.reset();

                            kv_or = KV::openOrCreate(KVOptions{
                                .filesystem_implementation = std::make_shared<PosixFileSystem>(temp_dir.path()),
                                .logger = logger,
                                .identifier = "test-kv-map",
                                .compact_after = 0,
                            });
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