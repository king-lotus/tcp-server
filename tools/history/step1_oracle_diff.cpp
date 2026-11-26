// Step 1 proof: before the old conn_sm is deleted from the header, every cell of the
// generated transition function is diffed against it (and against the relocated spec).
// Needs the PRE-migration header (the one that still has industrial::conn_sm), kept beside
// this file. Build from the repository root:
//   g++ -std=c++17 -O1 -fsanitize=address,undefined -Itools/history \
//       -DCONN_FSM_HEADER='"history/tcp-server-mt.pre-migration.h"' \
//       tools/history/step1_oracle_diff.cpp -o step1_diff && ./step1_diff
// fragment.inc is the generated region as the generator emitted it at that point.
#include <set>
#include "tcp-server-mt.pre-migration.h"
#include "../conn_fsm_model.h"

namespace gen {
using namespace industrial;
#include "fragment.inc"
}

CONN_FSM_ORACLE(HeaderOracle, industrial::conn_state, industrial::conn_sm);

int main() {
    using namespace model;
    long bad = 0, compared = 0;
    auto fail = [&](const std::string& m) { if (++bad <= 20) std::printf("  MISMATCH: %s\n", m.c_str()); };

    const Derived spec = derive_all<SpecOracle>();
    const Derived hdr = derive_all<HeaderOracle>();
    if (!hdr.errors.empty()) for (auto& e : hdr.errors) fail("header oracle: " + e);
    if (spec.states.size() != 77 || hdr.states.size() != 77) fail("state count");
    if (spec.cells.size() != hdr.cells.size()) fail("cell count differs between spec and header oracle");

    // 1. the relocated spec and the header's own conn_sm derive identical tables
    for (size_t i = 0; i < std::min(spec.cells.size(), hdr.cells.size()); ++i) {
        const Cell &a = spec.cells[i], &b = hdr.cells[i];
        ++compared;
        if (a.from != b.from || a.e != b.e || a.g != b.g || a.to != b.to || a.action != b.action || a.fx != b.fx)
            fail(std::string("spec vs header-old at ") + ename(a.e) + "/" + gname(a.g) + " from " + state_name(spec.states[a.from]));
    }
    std::printf("spec vs header's current conn_sm: %ld cells compared\n", compared);

    // 2. the generated switch equals every one of them, in-domain and out
    long gen_cells = 0, out_of_domain = 0;
    std::set<std::tuple<int, int, int>> seen;
    for (const Cell& c : hdr.cells) {
        seen.insert(std::make_tuple(c.from, static_cast<int>(c.e), static_cast<int>(c.g)));
        const auto r = gen::transition(static_cast<gen::conn_state_id>(c.from), static_cast<gen::conn_event>(static_cast<int>(c.e)),
                                       static_cast<gen::guard>(static_cast<int>(c.g)));
        ++gen_cells;
        if (static_cast<int>(r.next) != c.to || r.action != c.action || r.fx != c.fx)
            fail(std::string("generated switch at ") + ename(c.e) + "/" + gname(c.g) + " from " + state_name(hdr.states[c.from]));
    }
    for (int s = 0; s < 77; ++s) for (int ei = 0; ei < kEvents; ++ei) for (G gg : guards_of(static_cast<E>(ei))) {
        const int gi = static_cast<int>(gg);
        if (seen.count(std::make_tuple(s, ei, gi))) continue;
        const auto r = gen::transition(static_cast<gen::conn_state_id>(s), static_cast<gen::conn_event>(ei), static_cast<gen::guard>(gi));
        ++out_of_domain;
        if (static_cast<int>(r.next) != s || r.action != close_action::none || r.fx != 0) fail("out-of-domain cell is not inert");
    }
    std::printf("generated transition vs header's current conn_sm: %ld in-domain cells, %ld out-of-domain cells inert\n", gen_cells, out_of_domain);

    // 3. accessors against the axes
    for (int s = 0; s < 77; ++s) {
        const Axes& a = hdr.states[s]; const auto id = static_cast<gen::conn_state_id>(s);
        if (gen::phase_of(id) != a.phase || gen::read_of(id) != a.read || gen::write_armed_of(id) != a.write ||
            gen::in_round_of(id) != a.in_round || gen::round_of(id) != a.round || std::string(gen::name_of(id)) != state_name(a))
            fail("accessor for " + state_name(a));
    }
    std::printf("accessors: 77 states x 5 axes + names checked\n");
    std::printf("%s (%ld mismatches)\n", bad ? "FAILED" : "AGREE cell-for-cell", bad);
    return bad != 0;
}
