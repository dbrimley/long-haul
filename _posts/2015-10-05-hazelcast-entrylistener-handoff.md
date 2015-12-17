---
layout: post
title: "Hazelcast Entry Listener Hand-off"
modified:
categories: blog
excerpt: Hazelcast Entry Listeners are run on the eventing thread pool.  It's generally not recommended to have long running tasks execute here, in this article I'll present a generic execution framework for handling events.
tags: []
comments: false
image: handoff.jpg
date: 2015-10-05T11:20:13+00:00
---

One useful feature of Hazelcast is the ability to register a callback for just about any conceivable event that may occur in a cluster.  This ranges from mutations on the various data structures right through to listening for members leaving and joining the cluster.

Using the various callback interfaces it is possible to execute code that reacts in some way to these events.  For example performing some kind of processing in reaction to an entry being added to a [Map](http://docs.hazelcast.org/docs/latest/manual/html-single/hazelcast-documentation.html#map-listener).

## Treat Hazelcast eventing threads with care.

Most IMDG products will give you the same advice when dealing with user defined callbacks that run on system thread pools.  They'll tell you to only run short lived code within the callback.  This is because the eventing thread pools are often a finite resource, whereby if you do run code that takes minutes to complete you will encounter the risk of blocking other events, and at worst lose events completely.

If you're a newcomer to Hazelcast eventing threads you can get a primer [here](http://docs.hazelcast.org/docs/3.5/manual/html-single/hazelcast-documentation.html#event-threading)

I came across a situation recently where a Hazelcast user was running extremely complicated code within an [EntryListener](http://docs.hazelcast.org/docs/latest/javadoc/index.html?com/hazelcast/core/EntryListener.html), it was calling off to multiple helper classes that were performing all sorts of locking behaviors on other Hazelcast data structures, which in itself can be problematic.  The user complained that they were not seeing their callback code being executed after a certain time.  Further examination of the logs showed that the user created code was very long running and did in fact have a deadlock, this eventually lead to all the threads becoming blocked and subsequent events were backing up in memory.

So the advice stands, do not run complex and/or long running code off the event threading pool.  The answer is to off-load the execution onto a user controlled thread pool.

## The Entry Listener hand-off.

To demonstrate this I came up with a generic framework that would enable the user to easily create event processing code that could be linked to a Hazelcast map and its events.  In a nutshell it takes a Map based EntryEvent and executes it on a user configurable event pool.

All of the source code can be found at [https://github.com/dbrimley/hazelcast-entrylistener-handoff](https://github.com/dbrimley/hazelcast-entrylistener-handoff)

## EntryEventService

The Framework resides around the central concept of an [EntryEventService](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/EntryEventService.java).  The EntryEventService is responsible for processing the [EntryEvent](http://docs.hazelcast.org/docs/latest/javadoc/index.html?com/hazelcast/core/EntryEvent.html) that are passed to EntryListeners.

{% highlight java %}
package com.hazelcast.samples.entrylistener.handoff.service;

import com.hazelcast.samples.entrylistener.handoff.service.listener.CompletionListener;
import com.hazelcast.core.EntryEvent;

/**
 * Framework to provide encapsulation of EntryEvent processing.  Can be used to provide offloading of processing from
 * Hazelcast event threads.  EntryEvents are passed off to the EntryEventService via the process method.
 * <p>
 * The EntryEventService then selects the appropriate EntryEventProcessor.
 * <p>
 * If an EntryEvent is passed for processing and a EntryEventTypeProcessor is not found for that EntryEventType
 * then a EntryEventServiceException is passed to the caller via the CompletionListener.onException()
 */
public interface EntryEventService<K,V,R> {

    void process(EntryEvent<K,V> entryEvent, CompletionListener<R> completionListener);

}

{% endhighlight %}

The EntryEventService has one method called process.  This method takes the EntryEvent passed by Hazelcast and a [CompletionListener](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/listener/CompletionListener.java). The EntryEventService is responsible for examining the EntryEvent and processing it appropriately.

{% highlight java %}
package com.hazelcast.samples.entrylistener.handoff.service.listener;

import com.hazelcast.samples.entrylistener.handoff.service.exceptions.EntryEventServiceException;

/**
 * The CompletionListener is called by the EntryEventService either upon completion of a EntryEventProcessor or
 * if there are any Exceptions thrown in the course of execution or submission.
 */
public interface CompletionListener<R> {

    public void onCompletion(R result);

    public void onException(EntryEventServiceException e);

}
{% endhighlight %}


## EntryEventServiceDelegate

[EntryEventServiceDelegate](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/EntryEventServiceDelegate.java) is a Utility class takes care of handing off between the EntryListener and the EntryEventService.  __You should configure this class to be the EntryListener on your maps.__

{% highlight java %}
package com.hazelcast.samples.entrylistener.handoff.service;

import com.hazelcast.samples.entrylistener.handoff.service.listener.CompletionListener;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;

/**
 * Delegates calls to an EntryListener onto an EntryEventService
 */
public class EntryEventServiceDelegate implements EntryListener<Integer,String> {

    private EntryEventService entryEventService;
    private CompletionListener<Integer> completionListener;


    public EntryEventServiceDelegate(EntryEventService entryEventService, CompletionListener<Integer> completionListener) {
        this.entryEventService = entryEventService;
        this.completionListener = completionListener;
    }

    @Override
    public void entryAdded(EntryEvent<Integer, String> entryEvent) {
        entryEventService.process(entryEvent,completionListener);
    }

    @Override
    public void entryRemoved(EntryEvent<Integer, String> entryEvent) {
        entryEventService.process(entryEvent,completionListener);
    }

    @Override
    public void entryUpdated(EntryEvent<Integer, String> entryEvent) {
        entryEventService.process(entryEvent,completionListener);
    }

    @Override
    public void entryEvicted(EntryEvent<Integer, String> entryEvent) {
        entryEventService.process(entryEvent,completionListener);
    }

    @Override
    public void mapEvicted(MapEvent mapEvent) {
        throw new UnsupportedOperationException("MapEvents not supported by EntryEventService");
    }

    @Override
    public void mapCleared(MapEvent mapEvent) {
        throw new UnsupportedOperationException("MapEvents not supported by EntryEventService");
    }
}
{% endhighlight %}

## Different Processors per event type.

Some implementations can make use of the [EntryEventTypeProcessorFactory](/https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/processors/EntryEventTypeProcessorFactory.java).  This Factory provides an [EntryEventProcessor](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/processors/EntryEventProcessor.java) per EntryEventType, such as updated, added, removed, evicted etc.

## ThreadPoolEntryEventService

![EntryEventService](/images/EntryEventService.jpg)

There is currently one implementation of an EntryEventService, called the [ThreadPoolEntryEventService](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/service/ThreadPoolEntryEventService.java), it provides the following features :-

 1. Striped set of ThreadPoolExecutors that guarantees execution order by key.
 2. A Warning Service to inform of long running EntryEventTypeProcessors
 3. CompletionService callback to inform of failed/completed processes
 4. Getter to retrieve the striped queues of waiting EntryEvents

{% highlight java %}
package com.hazelcast.samples.entrylistener.handoff.service;

import com.hazelcast.samples.entrylistener.handoff.service.exceptions.EntryEventServiceException;
import com.hazelcast.samples.entrylistener.handoff.service.listener.CompletionListener;
import com.hazelcast.samples.entrylistener.handoff.service.listener.LongRunningThreadListener;
import com.hazelcast.samples.entrylistener.handoff.service.processors.EntryEventProcessor;
import com.hazelcast.samples.entrylistener.handoff.service.processors.EntryEventTypeProcessorFactory;
import com.hazelcast.core.EntryEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * EntryEventProcessors should be used to hand off EntryEvent processing from the Hazelcast Event Threads.  It is generally
 * considered bad practice to have potentially blocking and/or long running code hanging off the Hazelcast Event Thread.
 * <p/>
 * ThreadPoolEntryEventProcessor provides a striped set of ThreadPoolExecutors that guarantees execution order by key.
 * <ul>
 * <li>A Warning Service to inform of long running EntryEventTypeProcessors</li>
 * <li>CompletionService callback to inform of failed/completed processes</li>
 * <li>Getter to retrieve the striped queues of waiting EntryEvents</li>
 * </ul>
 */
public class ThreadPoolEntryEventService<K, V, R> implements EntryEventService<K,V,R>{

    public static final TimeUnit EXECUTOR_THREAD_TTL_TIMEUNIT = TimeUnit.HOURS;
    public static final long EXECUTOR_THREAD_TTL = 1;

    private final int numberOfExecutors;
    private final LongRunningThreadListener longRunningThreadListener;
    private final EntryEventTypeProcessorFactory entryEventTypeProcessorFactory;

    // List of the CompletionServices that are backed by an ExecutorService
    private List<ThreadPoolExecutor> executorList = new ArrayList();

    public ThreadPoolEntryEventService(int numberOfExecutors,
                                       int numberOfThreadsPerExecutor,
                                       int executorQueueCapacity,
                                       LongRunningThreadListener longRunningThreadListener,
                                       EntryEventTypeProcessorFactory entryEventTypeProcessorFactory) {

        this.numberOfExecutors = numberOfExecutors;
        this.longRunningThreadListener = longRunningThreadListener;
        this.entryEventTypeProcessorFactory = entryEventTypeProcessorFactory;

        setUpExecutors(numberOfExecutors, numberOfThreadsPerExecutor, executorQueueCapacity);
    }

    private void setUpExecutors(int numberOfExecutors, int numberOfThreadsPerExecutor, int queueCapacity) {
        for (int arrayIndex = 0; arrayIndex < numberOfExecutors; arrayIndex++) {
            ThreadPoolExecutor threadPoolExecutor = new TimedThreadPoolExector(numberOfThreadsPerExecutor,
                    numberOfThreadsPerExecutor, EXECUTOR_THREAD_TTL, EXECUTOR_THREAD_TTL_TIMEUNIT,
                    new ArrayBlockingQueue(queueCapacity), longRunningThreadListener);

            executorList.add(arrayIndex, threadPoolExecutor);
        }
    }

    /**
     * Process the EntryEvent using the provided EntryEventCallable.
     * Throws EntryEventProcessorException if the queue is full.
     *
     * @param entryEvent
     * @return R The result of the EntryEventCallable
     * @throws EntryEventServiceException
     */
    @Override
    public void process(EntryEvent<K, V> entryEvent, CompletionListener<R> completionListener) {

        ThreadPoolExecutor threadPoolExecutor = getThreadPoolExecutor(entryEvent);

        EntryEventProcessor<K, V, R> entryEventProcessor;

        try {
            entryEventProcessor = entryEventTypeProcessorFactory.getEntryEventTypeProcessor(entryEvent.getEventType());
        } catch (EntryEventServiceException e) {
            completionListener.onException(e);
            return;
        }

        CallableEntryEventTypeProcessor<K, V, R> callableEntryEventTypeProcessor = new CallableEntryEventTypeProcessor<>(
                completionListener, entryEvent, entryEventProcessor);

        submitEntryEventProcessor(completionListener, threadPoolExecutor, callableEntryEventTypeProcessor);

    }

    /**
     * Returns the Queue that a given key would be offered to. The EntryEventTypeProcessor representing this key
     * may or may not be present.
     *
     * @param key
     * @return BlockingQueue
     */
    public BlockingQueue getQueueForKey(K key) {
        return executorList.get(getKeyIndex(key)).getQueue();
    }

    private void submitEntryEventProcessor(CompletionListener<R> completionListener, ThreadPoolExecutor threadPoolExecutor,
                                           CallableEntryEventTypeProcessor<K, V, R> callableEntryEventTypeProcessor) {
        try {
            threadPoolExecutor.submit(callableEntryEventTypeProcessor);
        } catch (RejectedExecutionException e) {
            completionListener.onException(new EntryEventServiceException(e));
        }
    }

    private ThreadPoolExecutor getThreadPoolExecutor(EntryEvent<K, V> entryEvent) {
        int executorListIndex = getKeyIndex(entryEvent.getKey());
        return executorList.get(executorListIndex);
    }

    private int getKeyIndex(K key) {
        return Math.abs(key.hashCode() % numberOfExecutors);
    }

    private class TimedThreadPoolExector
            extends ThreadPoolExecutor {

        private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;
        private final LongRunningThreadListener longRunningThreadListener;
        private final Map<Runnable,ScheduledFuture> runningTasksMap = new ConcurrentHashMap<Runnable,ScheduledFuture>();


        public TimedThreadPoolExector(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                      BlockingQueue<Runnable> workQueue, LongRunningThreadListener longRunningThreadListener) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
            this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(corePoolSize);
            this.longRunningThreadListener = longRunningThreadListener;

        }

        @Override
        protected void beforeExecute(Thread t, Runnable r) {

            Date timeNow = new Date();
            long processTimeLimit = longRunningThreadListener.getProcessTimeLimit();
            TimeUnit processTimeLimitTimeUnit = longRunningThreadListener.getProcessTimeLimitTimeUnit();
            ScheduledFuture<?> scheduledFuture = scheduledThreadPoolExecutor
                    .scheduleAtFixedRate(new TimedTask(t, timeNow.getTime(), longRunningThreadListener),
                            processTimeLimit, processTimeLimit, processTimeLimitTimeUnit);

            runningTasksMap.put(r,scheduledFuture);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            ScheduledFuture scheduledFuture = runningTasksMap.get(r);
            scheduledFuture.cancel(true);
        }

    }

    private class TimedTask
            implements Runnable {

        private final long startTime;
        private final Thread thread;
        private final LongRunningThreadListener longRunningThreadListener;

        public TimedTask(Thread t, long time, LongRunningThreadListener longRunningThreadListener) {
            this.thread = t;
            this.startTime = time;
            this.longRunningThreadListener = longRunningThreadListener;
        }

        @Override
        public void run() {
            long timeNow = new Date().getTime();
            long elapsedTime = timeNow - startTime;
            longRunningThreadListener.onAlert(thread, elapsedTime);
            if (Thread.interrupted()){
                System.out.println("cancelling");
                return;
            }
        }

    }

}
{% endhighlight %}

## Spring based example

There is an example that is started by running [HazelcastBookCountOnWordsExample](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/java/com/hazelcast/samples/entrylistener/handoff/examples/HazelcastBookCountOnWordsExample.java)

This example bootstraps the Hazelcast Cluster Member by Spring config found at [application-context.xml](https://github.com/dbrimley/hazelcast-entrylistener-handoff/blob/master/src/main/resources/application-context.xml), it then loads 3 public domain books [AdventuresOfHuckleberryFinn.txt,Metamorphosis.txt,Ulysses.txt] into an IMap.  The IMap is keyed by title and the value is the text of the book.

There is an EntryListener attached to the map and when each book is put into the map it invokes a WordCountEntryEventProcessor via a ThreadPoolEntryEventService

#### Spring config example

{% highlight xml %}
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:hz="http://www.hazelcast.com/schema/spring" xmlns:util="http://www.springframework.org/schema/util"
       xsi:schemaLocation="http://www.springframework.org/schema/beans
                http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
                http://www.hazelcast.com/schema/spring
                http://www.hazelcast.com/schema/spring/hazelcast-spring.xsd http://www.springframework.org/schema/util http://www.springframework.org/schema/util/spring-util.xsd">

    <hz:hazelcast id="instance">
        <hz:config>
            <hz:group name="dev" password="password"/>
            <hz:network port="5701" port-auto-increment="true">
                <hz:join>
                    <hz:multicast enabled="true"
                                  multicast-group="224.2.2.3"
                                  multicast-port="54327"/>
                </hz:join>
            </hz:network>
            <hz:map name="testMap">
                <hz:entry-listeners>
                    <hz:entry-listener local="true" implementation="entryEventServiceDelegate"/>
                </hz:entry-listeners>
            </hz:map>
        </hz:config>
    </hz:hazelcast>

    <bean id="entryEventServiceDelegate" class="com.hazelcast.samples.entrylistener.handoff.service.EntryEventServiceDelegate">
        <constructor-arg index="0" ref="threadPoolEntryEventService"/>
        <constructor-arg index="1" ref="loggingCompletionListener"/>
    </bean>

    <bean id="threadPoolEntryEventService" class="com.hazelcast.samples.entrylistener.handoff.service.ThreadPoolEntryEventService">
        <!-- Number of Executors -->
        <constructor-arg index="0">
            <value>5</value>
        </constructor-arg>
        <!-- Number of Threads per Executor -->
        <constructor-arg index="1">
            <value>1</value>
        </constructor-arg>
        <!-- Queue Size of the Executors -->
        <constructor-arg index="2">
            <value>100</value>
        </constructor-arg>
        <!-- The Long Running Thread Listener -->
        <constructor-arg index="3" ref="loggingLongRunningThreadListener"/>
        <!-- The EntryEventProcessFactory -->
        <constructor-arg index="4" ref="entryEventTypeProcessorFactory"/>

    </bean>

    <bean id="loggingCompletionListener" class="com.hazelcast.samples.entrylistener.handoff.service.listener.LoggingCompletionListener"/>

    <bean id="loggingLongRunningThreadListener" class="com.hazelcast.samples.entrylistener.handoff.service.listener.LoggingLongRunningThreadListener">
        <constructor-arg index="0">
                <value>1</value>
        </constructor-arg>
        <constructor-arg index="1">
            <bean class="java.util.concurrent.TimeUnit" factory-method="valueOf">
                <constructor-arg value="MINUTES" />
            </bean>
        </constructor-arg>
    </bean>

    <!-- The EntryEventTypeProcessorFactory that is used by the EntryEventService -->
    <bean id="entryEventTypeProcessorFactory" class="com.hazelcast.samples.entrylistener.handoff.service.processors.EntryEventTypeProcessorFactory">
        <constructor-arg index="0">
            <map key-type="com.hazelcast.core.EntryEventType">
                <entry key="ADDED" value-ref="wordCountEntryEventProcessorClass"/>
            </map>
        </constructor-arg>
    </bean>

    <bean id="wordCountEntryEventProcessorClass" class="java.lang.Class" factory-method="forName">
        <constructor-arg index="0">
            <value>com.hazelcast.samples.entrylistener.handoff.service.processors.WordCountEntryEventProcessor</value>
        </constructor-arg>
    </bean>

</beans>
{% endhighlight %}

## Future enhancements

Possibly a static registry that can link maps to services would be a good step forward, presently we have to hard wire the delegate listener manually to the map.



