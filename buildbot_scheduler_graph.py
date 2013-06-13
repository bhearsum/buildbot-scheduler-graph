from collections import defaultdict
from copy import deepcopy
from imp import load_source
import json
import logging
import os
import os.path
import re
import sys

__version__ = "1.0"

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

chunked_builder_pattern = "(?P<basename>.*)[-_ ]\d+/\d+$"

def parse_schedulers(schedulers, triggerables={}):
    """Parses Scheduler data into a dict whose keys are the name of
       each Scheduler and whose values are a dict with the following keys:
        * nodes - A set of every node that needed to graph this Scheduler.
                  Generally this is the Scheduler itself, the builders it
                  notifies, and any upstream builders it may have.
        * edges - A set of relations between the nodes. There will be one edge
                  edge between the Scheduler and each of the builders that
                  it notifies. There is also an edge between the Scheduler and
                  each upstream that it may have.
    """
    graph_info = {}
    for s in schedulers:
        # Some schedulers have the same name as their builders, so let"s be sure
        # to avoid conflicts
        scheduler_name = "%s scheduler" % s.name
        log.info("%s: Creating graph", scheduler_name)
        graph = graph_info[scheduler_name] = defaultdict(set)
        graph["nodes"].add(scheduler_name)
        graph_info[scheduler_name]["root"] = True
        for builder in s.builderNames:
            log.info("%s: Adding Builder %s", scheduler_name, builder)
            graph["nodes"].add(builder)
            graph["edges"].add((scheduler_name, builder))
        if getattr(s, "trigger", None):
            log.debug("%s: Looking for Triggerable Builders", scheduler_name)
            graph_info[scheduler_name]["root"] = False
            for builder in triggerables.get(s.name, []):
                log.info("%s: Adding Builder %s", scheduler_name, builder)
                graph["nodes"].add(builder)
                graph["edges"].add((builder, scheduler_name))
        # Connect Dependent Schedulers together
        if getattr(s, "upstream_name", None):
            log.debug("%s: Connecting to Dependent Scheduler %s", scheduler_name, s.upstream_name)
            graph_info[scheduler_name]["root"] = False
            for upstream in schedulers:
                if upstream.name == s.upstream_name:
                    for builder in upstream.builderNames:
                        log.info("%s: Adding Builder %s", scheduler_name, builder)
                        graph["nodes"].add(builder)
                        graph["edges"].add((builder, scheduler_name))
        # Connect AggregatingScheduler Builders together
        if getattr(s, "upstreamBuilders", None):
            log.debug("%s: Adding Upstream Builders from Aggregating Scheduler", scheduler_name)
            graph_info[scheduler_name]["root"] = False
            for builder in s.upstreamBuilders:
                log.info("%s: Adding Builder %s", scheduler_name, builder)
                graph["nodes"].add(builder)
                graph["edges"].add((builder, scheduler_name))

    return graph_info


def merge_graph_info(graph_info):
    # We"re only going to be returning root Schedulers, but we need somewhere
    # to do all of the merging in the meantime, including merging one non-root
    # Scheduler into another.
    new_graph_info = deepcopy(graph_info)
    for s in graph_info:
        # Root Schedulers never need to be merged
        if graph_info[s]["root"] == True:
            pass

        log.info("%s: Evaluating for graph merging", s)
        # Any other Schedulers need to find the Scheduler that is upstream
        # of them.
        for other_s in graph_info:
            # If we see ourselves, abort!
            if s == other_s:
                continue
            log.debug("%s: Looking at nodes from %s", s, other_s)
            # If the upstream side of any edge appears in another Scheduler"s
            # builders, we should merge this one into it.
            for edge in graph_info[s]["edges"]:
                if edge[0] in new_graph_info[other_s]["nodes"]:
                    log.info("%s: Found matching node in %s, merging", s, other_s)
                    new_graph_info[other_s]["nodes"].update(new_graph_info[s]["nodes"])
                    new_graph_info[other_s]["edges"].update(new_graph_info[s]["edges"])
                    break
            else:
                log.debug("%s: No matching nodes, not merging", s)

    return {s:info for s,info in new_graph_info.iteritems() if info["root"]}


def merge_nodes(orig_nodes, orig_edges, merge_pattern=chunked_builder_pattern):
    transformations = set()
    r = re.compile(merge_pattern)
    # Organize the nodes and edges to make them easier to work with
    node_groups = defaultdict(list)
    node_edges = defaultdict(list)
    for n in orig_nodes:
        m = r.match(n)
        if m:
            basename = m.groupdict()["basename"]
            node_groups[basename].append(n)
    for e in orig_edges:
        node_edges[e[0]].append(e)
        node_edges[e[1]].append(e)

    # Do the merging, group by group.
    for basename, nodes in node_groups.iteritems():
        # Can't merge a group with only one item!
        if len(nodes) < 2:
            continue

        mergeable = True
        log.info("%s: Trying to merge node group", basename)
        # Nodes can only be merged together if all nodes in the group have
        # the same edges.
        required_edges = node_edges[nodes[0]]
        for n in nodes[1:]:
            if len(node_edges[n]) != len(required_edges):
                log.info("%s: Number of edges is different than %s", basename, n)
                mergeable = False
                break
            # Replace the current node with the first node in all of the edges, for purposes of easy comparison
            replaced_edges = [(left.replace(n, nodes[0]), right.replace(n, nodes[0])) for left, right in node_edges[n]]
            if set(required_edges) != set(replaced_edges):
                log.info("%s: Edge content is different than %s", basename, n)
                log.debug("%s: %s vs %s", basename, required_edges, replaced_edges)
                mergeable = False
                break

        # If the group is mergeable we need to add the basename to the list of
        # nodes, and add the deduplicated edges with the new node name.
        if mergeable:
            log.info("%s: Mergable!", basename)
            for n in nodes:
                transformations.add((n, basename))

    log.debug("Performing transformations: %s", transformations)
    merged_nodes = set()
    merged_edges = set()
    for n in orig_nodes:
        for before, after in transformations:
            n = n.replace(before, after)
        merged_nodes.add(n)
    for e in orig_edges:
        for before, after in transformations:
            e = (e[0].replace(before, after), e[1].replace(before, after))
        merged_edges.add(e)

    return merged_nodes, merged_edges

def main():
    from argparse import ArgumentParser
    import pydot

    parser = ArgumentParser()
    parser.add_argument("master_cfg", nargs=1)
    parser.add_argument("output_dir", nargs=1)
    parser.add_argument("-v", "--verbose", dest="verbose", action="count", default=0)
    parser.add_argument("-t", "--triggerables", dest="triggerables")

    args = parser.parse_args()
    master_cfg = os.path.abspath(args.master_cfg[0])
    output_dir = args.output_dir[0]
    if args.triggerables:
        triggerables = json.load(open(args.triggerables))
    else:
        triggerables = {}

    if args.verbose == 1:
        log.setLevel(logging.INFO)
    elif args.verbose >= 2:
        log.setLevel(logging.DEBUG)

    curdir = os.path.abspath(os.curdir)
    try:
        os.chdir(os.path.dirname(master_cfg))
        # Put the current directory in sys.path, in case there are imported
        # files there.
        sys.path.insert(0, "")
        cfg = load_source("cfg", master_cfg)

        graph_info = parse_schedulers(cfg.c["schedulers"], triggerables=triggerables)
        for name, graph_info in merge_graph_info(graph_info).iteritems():
            graph_info["nodes"], graph_info["edges"] = merge_nodes(graph_info["nodes"], graph_info["edges"])
            graph = pydot.Dot(graph_type="digraph")
            for node in graph_info["nodes"]:
                graph.add_node(pydot.Node(node))
            for edge in graph_info["edges"]:
                graph.add_edge(pydot.Edge(*edge))
            graph.write_png(os.path.join(output_dir, "%s.png" % name))

    finally:
        os.chdir(curdir)


if __name__ == "__main__":
    main()
