#include <catch2/catch_test_macros.hpp>
#include <memory>
#include <filesystem>
#include "kv/kv.hpp"
#include "filesystem/posixFileSystem.hpp"

using namespace aws::gg;
using namespace aws::gg::kv;

SCENARIO("I can create a KV map", "[kv]") {
    GIVEN("I create an empty KV map") {
        std::filesystem::remove_all(std::filesystem::current_path() / "test-kv-map");
        auto kv_or = KV::openOrCreate(KVOptions{
            .filesystem_implementation = std::make_shared<PosixFileSystem>(std::filesystem::current_path() / "test-kv-map"),
            .identifier = "test-kv-map",
            .compact_after = 0,
        });
        REQUIRE(kv_or);
    }
}