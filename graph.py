import os
import os.path
import pydot

def graph_objects(schedulers):
    scheduler_graph = pydot.Dot(graph_type='digraph')
    for s in schedulers:
        # Some schedulers have the same name as their builders, so let's be sure
        # to avoid conflicts
        scheduler_name = "%s" % s.name
        print "Adding Scheduler: %s" % scheduler_name
        scheduler_graph.add_node(pydot.Node(scheduler_name, label=scheduler_name))
        for builder in s.builderNames:
            print "  Adding Builder: %s" % builder
            scheduler_graph.add_node(pydot.Node(builder, label=builder))
            scheduler_graph.add_edge(pydot.Edge(scheduler_name, builder))
        # Connect Dependent Schedulers together
        if getattr(s, "upstream_name", None):
            print "  Adding Dependent Scheduler: %s" % s.upstream_name
            scheduler_graph.add_edge(pydot.Edge(s.upstream_name, scheduler_name))
        # Connect AggregatingScheduler Builders together
        if getattr(s, "upstreamBuilders", None):
            print "  Adding Builders from an AggregatingScheduler:"
            for builder in s.upstreamBuilders:
                print "    %s" % builder
                scheduler_graph.add_edge(pydot.Edge(builder, scheduler_name))

    return scheduler_graph


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('master_cfg', nargs=1)
    parser.add_argument('graph_file', nargs=1)

    args = parser.parse_args()
    master_cfg = os.path.abspath(args.master_cfg[0])
    graph_file = args.graph_file[0]

    curdir = os.path.abspath(os.curdir)
    try:
        os.chdir(os.path.dirname(master_cfg))
        cfg = {}
        execfile(master_cfg, cfg)

        graph = graph_objects(cfg['c']['schedulers'])
        graph.write_png(graph_file)
    finally:
        os.chdir(curdir)
