from collections import defaultdict
from functools import partial
import os
import os.path
import pydot

def graph_objects(schedulers):
    graph_info = {}
    for s in schedulers:
        # Some schedulers have the same name as their builders, so let's be sure
        # to avoid conflicts
        scheduler_name = "%s scheduler" % s.name
        print "Creating graph for Scheduler: %s" % scheduler_name
        graph = graph_info[scheduler_name] = defaultdict(set)
        graph['nodes'].add(scheduler_name)
        for builder in s.builderNames:
            print "  Adding Builder: %s" % builder
            graph['builders'].add(builder)
            graph['nodes'].add(builder)
            graph['edges'].add((scheduler_name, builder))
        # Connect Dependent Schedulers together
        if getattr(s, "upstream_name", None):
            print "  Adding Dependent Scheduler: %s" % s.upstream_name
            graph['edges'].add((s.upstream_name, scheduler_name))
        # Connect AggregatingScheduler Builders together
        if getattr(s, "upstreamBuilders", None):
            print "  Adding Builders from an AggregatingScheduler:"
            for builder in s.upstreamBuilders:
                if builder not in graph['builders']:
                    print "    %s" % builder
                    graph['builders'].add(builder)
                    graph['nodes'].add(builder)
                    graph['edges'].add((builder, scheduler_name))

    # Each root Scheduler should have its own graph. Any schedulers
    # which are downstream of something else should be put in that graph.
    # To do this, we need to inspect the graph information we have and combine
    # any schedulers which are downstream of something into their upstream
    # graph.
    def merge_schedulers():
        merged = set()
        for s in graph_info:
            for b in graph_info[s].get('builders', []):
                for other_s in graph_info:
                    if s == other_s:
                        continue
                    if b in graph_info[other_s].get('builders', []):
                        print "Merging %s into %s" % (s, other_s)
                        merged.add(s)
                        graph_info[other_s]['nodes'].update(graph_info[s]['nodes'])
                        graph_info[other_s]['edges'].update(graph_info[s]['edges'])
        for s in merged:
            del graph_info[s]
        return bool(merged)

    while merge_schedulers():
        pass

    print "Done merging"
    import sys
    sys.stdout.flush()
    graphs = defaultdict(partial(pydot.Dot, graph_type='digraph'))
    for s in graph_info:
        for node in graph_info[s]['nodes']:
            graphs[s].add_node(pydot.Node(node))
        for edge in graph_info[s]['edges']:
            graphs[s].add_edge(pydot.Edge(*edge))

    return graphs

if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('master_cfg', nargs=1)
    parser.add_argument('output_dir', nargs=1)

    args = parser.parse_args()
    master_cfg = os.path.abspath(args.master_cfg[0])
    output_dir = args.output_dir[0]

    curdir = os.path.abspath(os.curdir)
    try:
        os.chdir(os.path.dirname(master_cfg))
        cfg = {}
        execfile(master_cfg, cfg)

        for name, graph in graph_objects(cfg['c']['schedulers']).iteritems():
            graph.write_png(os.path.join(output_dir, "%s.png" % name))
    finally:
        os.chdir(curdir)
