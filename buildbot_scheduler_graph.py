from collections import defaultdict
from functools import partial
from imp import load_source
import json
import logging
import os
import os.path
import sys

__version__ = '1.0'

logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def graph_objects(schedulers, triggerables={}):
    import pydot
    graph_info = {}
    for s in schedulers:
        # Some schedulers have the same name as their builders, so let's be sure
        # to avoid conflicts
        scheduler_name = "%s scheduler" % s.name
        log.debug("Creating graph for Scheduler: %s" % scheduler_name)
        graph = graph_info[scheduler_name] = defaultdict(set)
        graph['nodes'].add(scheduler_name)
        graph_info[scheduler_name]['root'] = True
        for builder in s.builderNames:
            log.debug("  Adding Builder: %s" % builder)
            graph['builders'].add(builder)
            graph['nodes'].add(builder)
            graph['edges'].add((scheduler_name, builder))
        if getattr(s, "trigger", None):
            if s.name in triggerables:
                log.debug("  Hooking up Triggerable Scheduler Builders")
            for builder in triggerables.get(s.name, []):
                graph['edges'].add((builder, scheduler_name))
        # Connect Dependent Schedulers together
        if getattr(s, "upstream_name", None):
            log.debug("  Adding Dependent Scheduler: %s" % s.upstream_name)
            graph_info[scheduler_name]['root'] = False
            graph['edges'].add((s.upstream_name, scheduler_name))
        # Connect AggregatingScheduler Builders together
        if getattr(s, "upstreamBuilders", None):
            log.debug("  Adding Builders from an AggregatingScheduler:")
            graph_info[scheduler_name]['root'] = False
            for builder in s.upstreamBuilders:
                if builder not in graph['builders']:
                    log.debug("    %s" % builder)
                    graph['builders'].add(builder)
                    graph['nodes'].add(builder)
                    graph['edges'].add((builder, scheduler_name))

    # Each root Scheduler should have its own graph. Any schedulers
    # which are downstream of something else should be put in that graph.
    # To do this, we need to inspect the graph information we have and combine
    # any schedulers which are downstream of something into their upstream
    # graph.
    n = 1
    def merge_schedulers():
        merged = set()
        log.debug("Merge pass #%d" % n)
        for s in graph_info:
            for b in graph_info[s].get('builders', []):
                for other_s in graph_info:
                    if s == other_s:
                        continue
                    if b in graph_info[other_s].get('builders', []):
                        log.debug("Merging %s into %s" % (s, other_s))
                        graph_info[other_s]['nodes'].update(graph_info[s]['nodes'])
                        graph_info[other_s]['edges'].update(graph_info[s]['edges'])
                        merged.add(s)
        for s in merged:
            del graph_info[s]
        return bool(merged)

    while merge_schedulers():
        n += 1

    log.debug("Done merging")
    graphs = defaultdict(partial(pydot.Dot, graph_type='digraph'))
    for s in graph_info:
        if True:#graph_info[s]['root']:
            for node in graph_info[s]['nodes']:
                graphs[s].add_node(pydot.Node(node))
            for edge in graph_info[s]['edges']:
                graphs[s].add_edge(pydot.Edge(*edge))

    return graphs


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('master_cfg', nargs=1)
    parser.add_argument('output_dir', nargs=1)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
    parser.add_argument('-t', '--triggerables', dest='triggerables')

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
        cfg = load_source('cfg', master_cfg)

        for name, graph in graph_objects(cfg.c['schedulers'], triggerables=triggerables).iteritems():
            graph.write_png(os.path.join(output_dir, "%s.png" % name))
    finally:
        os.chdir(curdir)


if __name__ == '__main__':
    main()
