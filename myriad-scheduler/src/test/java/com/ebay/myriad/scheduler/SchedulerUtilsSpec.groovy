package com.ebay.myriad.scheduler

import com.ebay.myriad.state.NodeTask
import com.ebay.myriad.state.SchedulerState
import org.apache.mesos.Protos
import spock.lang.Specification

/**
 *
 * @author kensipe
 */
class SchedulerUtilsSpec extends Specification {

    def "is unique host name"() {
        given:
        def offer = Mock(Protos.OfferOrBuilder)
        offer.getHostname() >> "hostname"

        expect:
        returnValue == SchedulerUtils.isUniqueHostname(offer, tasks)

        where:
        tasks                                              | returnValue
        []                                                 | true
        null                                               | true
        createNodeTaskList("hostname")                     | false
        createNodeTaskList("missinghost")                  | true
        createNodeTaskList("missinghost1", "missinghost2") | true
        createNodeTaskList("missinghost1", "hostname")     | false

    }

    def "is eligible for FGS"() {
        given:
        def state = Mock(SchedulerState)
        def tasks = new ArrayList<NodeTask>()
        def fgsNMTask = new NodeTask(new NMProfile("zero", 0, 0))
        def cgsNMTask = new NodeTask(new NMProfile("low", 2, 4096))
        fgsNMTask.setHostname("test_fgs_hostname");
        cgsNMTask.setHostname("test_cgs_hostname");
        tasks.add(fgsNMTask)
        tasks.add(cgsNMTask)
        state.getActiveTasks() >> tasks;

        expect:
        returnValue == SchedulerUtils.isEligibleForFineGrainedScaling(hostName, state);

        where:
        hostName            | returnValue
        "test_fgs_hostname" | true
        "test_cgs_hostname" | false
        "blah"              | false
        ""                  | false
        null                | false
    }

    ArrayList<NodeTask> createNodeTaskList(String... hostnames) {
        def list = []
        hostnames.each { hostname ->
            list << createNodeTask(hostname)
        }
        return list
    }


    NodeTask createNodeTask(String hostname) {
        def node = new NodeTask(new NMProfile("", 1, 1))
        node.hostname = hostname
        node
    }
}
