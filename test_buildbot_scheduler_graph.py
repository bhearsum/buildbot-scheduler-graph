import unittest

from buildbot_scheduler_graph import parse_schedulers, merge_graph_info, \
    merge_nodes

class Scheduler(object):
    def __init__(self, name, builderNames):
        self.name = name
        self.builderNames = builderNames

class Triggerable(Scheduler):
    trigger = True

class Dependent(Scheduler):
    def __init__(self, name, builderNames, upstream_name):
        super(Dependent, self).__init__(name, builderNames)
        self.upstream_name = upstream_name

class AggregatingScheduler(Scheduler):
    def __init__(self, name, builderNames, upstreamBuilders):
        super(AggregatingScheduler, self).__init__(name, builderNames)
        self.upstreamBuilders = upstreamBuilders


class TestCombineObjects(unittest.TestCase):
    def testSimpleScheduler(self):
        s = [Scheduler("foo", ("bar", "baz"))]
        expected = {
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "baz")),
                "edges": set((("foo scheduler", "bar"), ("foo scheduler", "baz"))),
                "root": True,
            }
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testTriggerableScheduler(self):
        s = [
            Scheduler("base", ("upstream",)),
            Triggerable("foo", ("bar",)),
        ]
        triggerables= {
            "foo": ("upstream",),
        }
        expected = {
            "base scheduler": {
                "nodes": set(("base scheduler", "upstream")),
                "edges": set((("base scheduler", "upstream"),)),
                "root": True,
            },
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "upstream")),
                "edges": set((("foo scheduler", "bar"), ("upstream", "foo scheduler"))),
                "root": False,
            }
        }
        self.assertEquals(parse_schedulers(s, triggerables), expected)

    def testDependentScheduler(self):
        s = [
            Scheduler("base", ("upstream",)),
            Dependent("foo", ("bar",), upstream_name="base"),
        ]
        expected = {
            "base scheduler": {
                "nodes": set(("base scheduler", "upstream")),
                "edges": set((("base scheduler", "upstream"),)),
                "root": True,
            },
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "upstream")),
                "edges": set((("foo scheduler", "bar"), ("upstream", "foo scheduler"))),
                "root": False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testDependentSchedulerMultipleUpstreamBuilders(self):
        s = [
            Scheduler("base", ("upstream1", "upstream2")),
            Dependent("foo", ("bar",), upstream_name="base"),
        ]
        expected = {
            "base scheduler": {
                "nodes": set(("base scheduler", "upstream1", "upstream2")),
                "edges": set((("base scheduler", "upstream1"), ("base scheduler", "upstream2"))),
                "root": True,
            },
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "upstream1", "upstream2")),
                "edges": set((("foo scheduler", "bar"), ("upstream1", "foo scheduler"), ("upstream2", "foo scheduler"))),
                "root": False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testAggregatingScheduler(self):
        s = [
            Scheduler("base", ("upstream",)),
            AggregatingScheduler("foo", ("bar",), upstreamBuilders=("upstream",)),
        ]
        expected = {
            "base scheduler": {
                "nodes": set(("base scheduler", "upstream")),
                "edges": set((("base scheduler", "upstream"),)),
                "root": True,
            },
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "upstream")),
                "edges": set((("foo scheduler", "bar"), ("upstream", "foo scheduler"))),
                "root": False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testAggergatingSchedulerMultipleUpstreamBuilders(self):
        s = [
            Scheduler("base", ("upstream1", "upstream2")),
            AggregatingScheduler("foo", ("bar",), upstreamBuilders=("upstream1", "upstream2")),
        ]
        expected = {
            "base scheduler": {
                "nodes": set(("base scheduler", "upstream1", "upstream2")),
                "edges": set((("base scheduler", "upstream1"), ("base scheduler", "upstream2"))),
                "root": True,
            },
            "foo scheduler": {
                "nodes": set(("foo scheduler", "bar", "upstream1", "upstream2")),
                "edges": set((("foo scheduler", "bar"), ("upstream1", "foo scheduler"), ("upstream2", "foo scheduler"))),
                "root": False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)


class TestMergeGraphInfo(unittest.TestCase):
    maxDiff = 2000
    def testSimpleMerge(self):
        graph_info = {
            "base": {
                "nodes": set(("base", "upstream")),
                "edges": set((("base", "upstream"),)),
                "root": True,
            },
            "foo": {
                "nodes": set(("foo", "bar", "upstream")),
                "edges": set((("foo", "bar"), ("upstream", "foo"))),
                "root": False,
            },
        }
        expected = {
            "base": {
                "nodes": set(("base", "upstream", "foo", "bar")),
                "edges": set((("base", "upstream"), ("upstream", "foo"), ("foo", "bar"))),
                "root": True,
            },
        }
        self.assertEquals(merge_graph_info(graph_info), expected)

    def testMultipleUpstreamMerge(self):
        graph_info = {
            "base": {
                "nodes": set(("base", "upstream1", "upstream2")),
                "edges": set((("base", "upstream1"), ("base", "upstream2"))),
                "root": True,
            },
            "foo": {
                "nodes": set(("foo", "bar", "upstream1", "upstream2")),
                "edges": set((("foo", "bar"), ("upstream1", "foo"), ("upstream2", "foo"))),
                "root": False,
            },
        }
        expected = {
            "base": {
                "nodes": set(("base", "upstream1", "upstream2", "foo", "bar")),
                "edges": set((("base", "upstream1"), ("base", "upstream2"), ("upstream1", "foo"), ("upstream2", "foo"), ("foo", "bar"))),
                "root": True,
            },
        }
        self.assertEquals(merge_graph_info(graph_info), expected)

    def testMergeWorksRegardlessOfOrder(self):
        from collections import OrderedDict
        graph_info = OrderedDict()
        graph_info["foo"] = {
            "nodes": set(("foo", "bar", "upstream1", "upstream2")),
            "edges": set((("foo", "bar"), ("upstream1", "foo"), ("upstream2", "foo"))),
            "root": False,
        }
        graph_info["base"] = {
            "nodes": set(("base", "upstream1", "upstream2")),
            "edges": set((("base", "upstream1"), ("base", "upstream2"))),
            "root": True,
        }
        expected = {
            "base": {
                "nodes": set(("base", "upstream1", "upstream2", "foo", "bar")),
                "edges": set((("base", "upstream1"), ("base", "upstream2"), ("upstream1", "foo"), ("upstream2", "foo"), ("foo", "bar"))),
                "root": True,
            },
        }
        self.assertEquals(merge_graph_info(graph_info), expected)

    def testMultilevelMerge(self):
        graph_info = {
            "base": {
                "nodes": set(("base", "basebuilder")),
                "edges": set((("base", "basebuilder"),)),
                "root": True,
            },
            "foo": {
                "nodes": set(("foo", "foobuilder", "basebuilder")),
                "edges": set((("foo", "foobuilder"), ("basebuilder", "foo"))),
                "root": False,
            },
            "bar": {
                "nodes": set(("bar", "barbuilder", "foobuilder")),
                "edges": set((("bar", "barbuilder"), ("foobuilder", "bar"))),
                "root": False,
            }
        }
        expected = {
            "base": {
                "nodes": set(("base", "basebuilder", "foo", "foobuilder",
                              "bar", "barbuilder")),
                "edges": set((("base", "basebuilder"), ("foo", "foobuilder"), ("basebuilder", "foo"),
                              ("bar", "barbuilder"),
                              ("foobuilder", "bar"))),
                "root": True,
            }
        }
        self.assertEquals(merge_graph_info(graph_info), expected)

    def testDiamondMerge(self):
        graph_info = {
            "base": {
                "nodes": set(("base", "basebuilder")),
                "edges": set((("base", "basebuilder"),)),
                "root": True,
            },
            "foo": {
                "nodes": set(("foo", "foobuilder", "basebuilder")),
                "edges": set((("foo", "foobuilder"), ("basebuilder", "foo"))),
                "root": False,
            },
            "bar": {
                "nodes": set(("bar", "barbuilder", "basebuilder")),
                "edges": set((("bar", "barbuilder"), ("basebuilder", "bar"))),
                "root": False,
            },
            "merged": {
                "nodes": set(("merged", "mergedbuilder", "foobuilder", "barbuilder")),
                "edges": set((("merged", "mergedbuilder"), ("foobuilder", "merged"), ("barbuilder", "merged"))),
                "root": False,
            }
        }
        expected = {
            "base": {
                "nodes": set(("base", "basebuilder", "foo", "foobuilder",
                              "bar", "barbuilder", "merged", "mergedbuilder")),
                "edges": set((("base", "basebuilder"), ("foo", "foobuilder"), ("basebuilder", "foo"),
                              ("bar", "barbuilder"), ("basebuilder", "bar"), ("merged", "mergedbuilder"),
                              ("foobuilder", "merged"), ("barbuilder", "merged"))),
                "root": True,
            }
        }
        self.assertEquals(merge_graph_info(graph_info), expected)


class TestMergeNodes(unittest.TestCase):
    def testMergeMultipleDownstream(self):
        nodes = {"base", "foo 1/2", "foo 2/2"}
        edges = {("base", "foo 1/2"), ("base", "foo 2/2")}
        expected = ({"base", "foo"}, {("base", "foo"),})
        self.assertEquals(merge_nodes(nodes, edges), expected)

    def testMergeMultipleUpstream(self):
        pass

    def testUnmergeableUpstreams(self):
        pass

    def testUnmergeableDownstreams(self):
        pass

    def testCustomMergePattern(self):
        pass
