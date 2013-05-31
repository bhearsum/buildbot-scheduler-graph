import unittest

from buildbot_scheduler_graph import parse_schedulers

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
        s = [Scheduler('foo', ('bar', 'baz'))]
        expected = {
            'foo': {
                'nodes': set(('foo', 'bar', 'baz')),
                'edges': set((('foo', 'bar'), ('foo', 'baz'))),
                'root': True,
            }
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testTriggerableScheduler(self):
        s = [
            Scheduler('base', ('upstream',)),
            Triggerable('foo', ('bar',)),
        ]
        triggerables= {
            'foo': ('upstream',),
        }
        expected = {
            'base': {
                'nodes': set(('base', 'upstream')),
                'edges': set((('base', 'upstream'),)),
                'root': True,
            },
            'foo': {
                'nodes': set(('foo', 'bar', 'upstream')),
                'edges': set((('foo', 'bar'), ('upstream', 'foo'))),
                'root': False,
            }
        }
        self.assertEquals(parse_schedulers(s, triggerables), expected)

    def testDependentScheduler(self):
        s = [
            Scheduler('base', ('upstream',)),
            Dependent('foo', ('bar',), upstream_name='base'),
        ]
        expected = {
            'base': {
                'nodes': set(('base', 'upstream')),
                'edges': set((('base', 'upstream'),)),
                'root': True,
            },
            'foo': {
                'nodes': set(('foo', 'bar', 'upstream')),
                'edges': set((('foo', 'bar'), ('upstream', 'foo'))),
                'root': False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testDependentSchedulerMultipleUpstreamBuilders(self):
        s = [
            Scheduler('base', ('upstream1', 'upstream2')),
            Dependent('foo', ('bar',), upstream_name='base'),
        ]
        expected = {
            'base': {
                'nodes': set(('base', 'upstream1', 'upstream2')),
                'edges': set((('base', 'upstream1'), ('base', 'upstream2'))),
                'root': True,
            },
            'foo': {
                'nodes': set(('foo', 'bar', 'upstream1', 'upstream2')),
                'edges': set((('foo', 'bar'), ('upstream1', 'foo'), ('upstream2', 'foo'))),
                'root': False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testAggregatingScheduler(self):
        s = [
            Scheduler('base', ('upstream',)),
            AggregatingScheduler('foo', ('bar',), upstreamBuilders=('upstream',)),
        ]
        expected = {
            'base': {
                'nodes': set(('base', 'upstream')),
                'edges': set((('base', 'upstream'),)),
                'root': True,
            },
            'foo': {
                'nodes': set(('foo', 'bar', 'upstream')),
                'edges': set((('foo', 'bar'), ('upstream', 'foo'))),
                'root': False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)

    def testAggergatingSchedulerMultipleUpstreamBuilders(self):
        s = [
            Scheduler('base', ('upstream1', 'upstream2')),
            AggregatingScheduler('foo', ('bar',), upstreamBuilders=('upstream1', 'upstream2')),
        ]
        expected = {
            'base': {
                'nodes': set(('base', 'upstream1', 'upstream2')),
                'edges': set((('base', 'upstream1'), ('base', 'upstream2'))),
                'root': True,
            },
            'foo': {
                'nodes': set(('foo', 'bar', 'upstream1', 'upstream2')),
                'edges': set((('foo', 'bar'), ('upstream1', 'foo'), ('upstream2', 'foo'))),
                'root': False,
            },
        }
        self.assertEquals(parse_schedulers(s), expected)
