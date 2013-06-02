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

chunked_builder_pattern = "(?P<basename>.*) \d+/\d+$"

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
        log.debug("Creating graph for Scheduler: %s" % scheduler_name)
        graph = graph_info[scheduler_name] = defaultdict(set)
        graph["nodes"].add(scheduler_name)
        graph_info[scheduler_name]["root"] = True
        for builder in s.builderNames:
            log.debug("  Adding Builder: %s" % builder)
            graph["nodes"].add(builder)
            graph["edges"].add((scheduler_name, builder))
        if getattr(s, "trigger", None):
            graph_info[scheduler_name]["root"] = False
            if s.name in triggerables:
                log.debug("  Hooking up Triggerable Scheduler Builders")
            for builder in triggerables.get(s.name, []):
                graph["nodes"].add(builder)
                graph["edges"].add((builder, scheduler_name))
        # Connect Dependent Schedulers together
        if getattr(s, "upstream_name", None):
            log.debug("  Adding Dependent Scheduler: %s" % s.upstream_name)
            graph_info[scheduler_name]["root"] = False
            for upstream in schedulers:
                if upstream.name == s.upstream_name:
                    for builder in upstream.builderNames:
                        graph["nodes"].add(builder)
                        graph["edges"].add((builder, scheduler_name))
        # Connect AggregatingScheduler Builders together
        if getattr(s, "upstreamBuilders", None):
            log.debug("  Adding Builders from an AggregatingScheduler:")
            graph_info[scheduler_name]["root"] = False
            for builder in s.upstreamBuilders:
                log.debug("    %s" % builder)
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

        log.debug("Evaluating %s for merging" % s)
        # Any other Schedulers need to find the Scheduler that is upstream
        # of them.
        for other_s in graph_info:
            # If we see ourselves, abort!
            if s == other_s:
                continue
            log.debug("  Looking at nodes from %s: %s" % (other_s, new_graph_info[other_s]))
            # If the upstream side of any edge appears in another Scheduler"s
            # builders, we should merge this one into it.
            for edge in graph_info[s]["edges"]:
                if edge[0] in new_graph_info[other_s]["nodes"]:
                    log.debug("    Found node %s, merging" % edge[0])
                    new_graph_info[other_s]["nodes"].update(new_graph_info[s]["nodes"])
                    new_graph_info[other_s]["edges"].update(new_graph_info[s]["edges"])
                    break

    return {s:info for s,info in new_graph_info.iteritems() if info["root"]}


def merge_nodes(nodes, edges, merge_pattern=chunked_builder_pattern):
    # A place to store the new sets of nodes and edges.
    merged_nodes = set()
    merged_edges = set()
    r = re.compile(merge_pattern)
    # Organize the nodes and edges to make them easier to work with
    node_groups = defaultdict(list)
    node_edges = defaultdict(list)
    for n in nodes:
        m = r.match(n)
        if m:
            basename = m.groupdict()["basename"]
            node_groups[basename].append(n)
        else:
            merged_nodes.add(n)
    for e in edges:
        node_edges[e[0]].append(e)
        node_edges[e[1]].append(e)

    # Do the merging, group by group.
    for basename, nodes in node_groups.iteritems():
        # Can't merge a group with only one item!
        if len(nodes) < 2:
            merged_nodes.update(nodes)
            continue

        mergeable = True
        log.debug("Trying to merge node group '%s'" % basename)
        # Nodes can only be merged together if all nodes in the group have
        # the same edges.
        required_edges = node_edges[nodes[0]]
        for n in nodes[1:]:
            log.debug("Comparing %s to %s" % (nodes[0], n))
            if len(node_edges[n]) != len(required_edges):
                log.debug("Number of edges differs between, can't merge")
                mergeable = False
                break
            # Replace the current node with the first node in all of the edges, for purposes of easy comparison
            replaced_edges = [(left.replace(n, nodes[0]), right.replace(n, nodes[0])) for left, right in node_edges[n]]
            if required_edges != replaced_edges:
                log.debug("Edge content differs (%s vs %s), can't merge" % (required_edges, replaced_edges))
                mergeable = False
                break

    # If the group is mergeable we need to add the basename to the list of
    # nodes, and add the deduplicated edges with the new node name.
    if mergeable:
        log.debug("Group is mergeable!")
        merged_nodes.add(basename)
        for n in nodes:
            for e in node_edges[n]:
                merged_edges.add((e[0].replace(n, basename), e[1].replace(n, basename)))
    # If the group isn't mergeable, just copy the original group data in.
    else:
        merged_nodes.update(nodes)
        for n in nodes:
            merged_edges.update(node_edges[n])

    return merged_nodes, merged_edges


def main():
    from argparse import ArgumentParser
    import pydot

    parser = ArgumentParser()
    parser.add_argument("master_cfg", nargs=1)
    parser.add_argument("output_dir", nargs=1)
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False)
    parser.add_argument("-t", "--triggerables", dest="triggerables")

    args = parser.parse_args()
    master_cfg = os.path.abspath(args.master_cfg[0])
    output_dir = args.output_dir[0]
    if args.triggerables:
        triggerables = json.load(open(args.triggerables))
    else:
        triggerables = {}

    if args.verbose:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    curdir = os.path.abspath(os.curdir)
    try:
        os.chdir(os.path.dirname(master_cfg))
        # Put the current directory in sys.path, in case there are imported
        # files there.
        sys.path.insert(0, "")
        cfg = load_source("cfg", master_cfg)

        graph_info = parse_schedulers(cfg.c["schedulers"], triggerables=triggerables)
        for name, graph_info in merge_graph_info(graph_info).iteritems():
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
