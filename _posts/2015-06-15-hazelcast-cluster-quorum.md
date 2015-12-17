---
layout: post
title: "Hazelcast Cluster Quorum"
modified:
categories: blog
excerpt: A new feature in the 3.5 release of Hazelcast is the Cluster Quorum.  You can use Cluster Quorums to restrict operations on Maps based upon environmental criteria.
tags: []
comments: true
image: gatekeeper.jpg
date: 2015-06-21T21:04:13+00:00
---
A new feature in the 3.5 release of Hazelcast is the Cluster Quorum.  In this instance we're not talking about a Quorum in its traditional distributed systems sense, think of a Cluster Quorum as a kind of gatekeeper, protecting your cluster during times of unexpected member loss. You can use Cluster Quorums to restrict operations on Maps or indeed the entire cluster based upon environmental criteria.

This sounds great you say, but I'm still not sure how this can help me?  OK. Lets take a look at a scenario...

Imagine a cluster that has a very high number of writes to a certain map.  We also have other maps that are not updated quite so frequently and all the while we have hundreds of clients all reading from the cluster but not at the same frequency as the data that is entering the system.

In normal circumstances if a machine or a number of machines were to die in the cluster we may still have enough memory available to store our data, but the amount of threads available to process requests would be reduced.  We now have less cores available and the partition threads in the cluster could quickly become overwhelmed by the one map that is updated rapidly.  This could mean other clients becoming starved of threads, unable to service requests.  It's also possible that the remaining members would become so consumed that they're unable to respond to membership pings, the knock on effect could result in the member being forced out of the cluster on the assumption that it is dead.

To protect the rest of the cluster in the event of member loss we need a way to stop the writes to the high frequency map whilst allowing operations to the other data structures.  We can then continue to provide a good service to our other users whilst the crashed machines are restored to the cluster.

## Bring on the Quorum!

As of Hazelcast 3.5 we now have the ability to restrict operations on distinct data structures.  We do this via a Quorum configuration.  We observed that other IMDG products provide Quorums that have protection at a cluster level,we decided to go one step further and provide Quorum protection around data structures as well.

In the example below we create a very simple Quorum on the `default` map.  The 'default' map in Hazelcast is the configuration used if no other match is found.  In this instance no operations will be allowed unless the cluster has a minimum of 3 members.  You'll also note that the Quorum configuration is separate from the Map.  This means that you can have multiple Quorums in a cluster attached to many different structures.

If the Quorum thresholds are not satisfied then a `QuorumException` is thrown when we try to interact with the `default` map in any way.  Be it from a client or another member.

{% highlight xml %}
<hazelcast>

  <quorum name="quorumRuleWithThreeNodes" enabled=true>
    <quorum-size>3</quorum-size>
  </quorum>

  <map name="default">
    <quorum-name>quorumRuleWithThreeNodes</quorum-name>
  </map>

</hazelcast>
{% endhighlight %}

## Quorum Functions

It's simple to set up a Quorum check based on cluster size as we've seen above, but if you want to make a slightly more complex check you can do this by applying a Quorum Function.

{% highlight java %}
  QuorumConfig quorumConfig = new QuorumConfig();
  quorumConfig.setName("MyQuorum");
  quorumConfig.setEnabled(true);
  quorumConfig.setType(QuorumType.WRITE);

  quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
    @Override
    public boolean apply(Collection<Member> members) {
      return (members.size() >= 3) && (someOtherExternalClusterState);
    }
  });
{% endhighlight %}

In the example above we use Configuration API to set-up the Quorum to disallow `writes` if the boolean returned from the `QuorumFunction` is false.  In the function we test if the size of the cluster is greater than 3 and also if a variable named `someOtherExternalClusterState` is equal true.

You now get the idea that by using a function you can test for other state and not just cluster member.

## Listen In.

Another nice feature of Quorums is the ability to listen in to Quorum Events.  You can register a new callback interface called not surprisingly a `QuorumListener`.  Quorum listeners are local to the node that they are registered, so they receive only events occurred on that local node.


{% highlight xml %}
<hazelcast>

  <quorum name="quorumRuleWithThreeNodes" enabled="true">
    <quorum-size>3</quorum-size>
    <quorum-listeners>
      <quorum-listener>com.company.quorum.ThreeNodeQuorumListener</quorum-listener>
    </quorum-listeners>
  </quorum>

  <map name="default">
    <quorum-name>quorumRuleWithThreeNodes</quorum-name>
  </map>

</hazelcast>
{% endhighlight %}

The `QuorumListener` has just one method that is called passing you a `QuorumEvent`.

{% highlight java %}
package com.hazelcast.quorum;

import java.util.EventListener;

/**
 * Listener to get notified when a quorum state is changed
 */
public interface QuorumListener extends EventListener {

    /**
     * Called when quorum presence state is changed.
     *
     * @param quorumEvent provides information about quorum presence and current member list.
     */
    void onChange(QuorumEvent quorumEvent);

}
{% endhighlight %}

The `QuorumEvent` itself allows you to determine if a Quorum has been established or if it has been lost via its `isPresent()` method call.  Additionally it provides the required cluster members to form a quorum and also the current membership list.

## Query the Quorums.

Above we saw how we could receive callbacks, but in some cases we may just wish to make an immediate check to see if the Quorum is established or not.  We can do this via the `QuorumService`.

{% highlight java %}
HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
QuorumService quorumService = hazelcastInstance.getQuorumService();
Quorum quorum = quorumService.getQuorum(quorumName);

boolean quorumPresence = quorum.isPresent();
{% endhighlight %}

## In Conclusion

The Cluster Quorum feature is another important tool for you to manage your cluster. In future versions of Hazelcast there are plans to add other data structures, for example you'll be able to protect operations against Topics or Queues.

