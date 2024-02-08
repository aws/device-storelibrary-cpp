#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <filesystem>
#include <string>
#include "kv/kv.hpp"
#include "filesystem/posixFileSystem.hpp"

using namespace aws::gg;
using namespace aws::gg::kv;
using namespace std::string_literals;

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        std::filesystem::remove_all(std::filesystem::current_path() / "test-kv-map");
        auto kv_or = KV::openOrCreate(KVOptions{
            .filesystem_implementation = std::make_shared<PosixFileSystem>(std::filesystem::current_path() / "test-kv-map"),
            .identifier = "test-kv-map",
            .compact_after = 0,
        });
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        auto key = "key"s;
        auto value = "value"s;
        WHEN("I add a value") {
            auto e = kv->put(key, BorrowedSlice{value});
            REQUIRE(e.code == KVErrorCodes::NoError);

            THEN("I can get the value back") {
                auto v_or = kv->get(key);
                REQUIRE(v_or);
                REQUIRE(v_or.val().string() == value);
            }
        }
    }
}