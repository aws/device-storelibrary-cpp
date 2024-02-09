#include "filesystem/posixFileSystem.hpp"
#include "kv/kv.hpp"
#include "test_utils.hpp"
#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <string_view>

using namespace aws::gg;
using namespace aws::gg::kv;

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        auto temp_dir = TempDir();
        auto kv_or = KV::openOrCreate(KVOptions{
            .filesystem_implementation = std::make_shared<PosixFileSystem>(temp_dir.path()),
            .identifier = "test-kv-map",
            .compact_after = 0,
        });
        REQUIRE(kv_or);

        auto kv = std::move(kv_or.val());

        const std::string &key = GENERATE(take(10, random(1, 512)));
        const std::string &value = GENERATE(take(2, random(1, 1 * 1024 * 1024)));

        WHEN("I add a value") {
            auto e = kv->put(key, BorrowedSlice{value});
            REQUIRE(e.code == KVErrorCodes::NoError);

            THEN("I can get the value back") {
                auto v_or = kv->get(key);
                REQUIRE(v_or);
                REQUIRE(std::string_view{v_or.val().char_data(), v_or.val().size()} == value);
            }
        }
    }
}