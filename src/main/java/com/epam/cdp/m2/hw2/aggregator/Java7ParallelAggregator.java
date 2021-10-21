package com.epam.cdp.m2.hw2.aggregator;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javafx.util.Pair;
import lombok.NonNull;

import static java.util.stream.Collectors.*;

public class Java7ParallelAggregator implements Aggregator {
    @Override
    public int sum(List<Integer> numbers) {
       // final int numOfThreads = 4;
        CountDownLatch countDownLatch = new CountDownLatch(numbers.size());
        ExecutorService executorService = Executors.newCachedThreadPool();
        AtomicInteger sum = new AtomicInteger(0);
        for (Integer i : numbers) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    sum.getAndAdd(i);
                    countDownLatch.countDown();
                }
            });

        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sum.get();
    }

    @Override
    public List<Pair<String, Long>> getMostFrequentWords(@NonNull List<String> words, long limit) {
        CountDownLatch countDownLatch = new CountDownLatch(words.size());
        ExecutorService executorService = Executors.newCachedThreadPool();
        Map<String,Long> frequentWordsMap = new ConcurrentHashMap<>();
        for (String s : words) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    String toLowerCase = s.toLowerCase(Locale.ROOT);
                    frequentWordsMap.merge(toLowerCase, 1L, Long::sum);
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<Pair<String, Long>> toSort = new ArrayList<>();
        CountDownLatch countDownLatchToSort = new CountDownLatch(frequentWordsMap.size());
        for (Map.Entry<String, Long> stringLongEntry : frequentWordsMap.entrySet()) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    synchronized (toSort)
                    {
                        toSort.add(new Pair<>(stringLongEntry.getKey(),stringLongEntry.getValue()));
                    }
                    countDownLatchToSort.countDown();
                }
            });
        }
        try {
            countDownLatchToSort.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Collections.sort(toSort, new Comparator<Pair<String, Long>>() {
            @Override
            public int compare(Pair<String, Long> o1, Pair<String, Long> o2) {
                long d;
                if((d = - o1.getValue() + o2.getValue())==0)
                    return o1.getKey().compareTo(o2.getKey());
                return (int) d;
            }
        });

        return toSort.subList(0,Math.min((int)limit,toSort.size()));


    }

    @Override
    public List<String> getDuplicates(List<String> words, long limit) {
        CountDownLatch countDownLatch = new CountDownLatch(words.size());
        ExecutorService executorService = Executors.newCachedThreadPool();
        Map<String,Long> frequentWordsMap = new ConcurrentHashMap<>();
        for (String s : words) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    String toUpperCase = s.toUpperCase(Locale.ROOT);
                    frequentWordsMap.merge(toUpperCase, 1L, Long::sum);
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        List<String> toSort = new ArrayList<>();
        CountDownLatch countDownLatchToSort = new CountDownLatch(frequentWordsMap.size());
        for (Map.Entry<String, Long> stringLongEntry : frequentWordsMap.entrySet()) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    synchronized (toSort)
                    {
                        if(stringLongEntry.getValue()>1)
                            toSort.add(stringLongEntry.getKey());
                    }
                    countDownLatchToSort.countDown();
                }
            });
        }
        try {
            countDownLatchToSort.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        toSort.sort((o1, o2) -> {
            int d;
            if((d=o1.length() - o2.length())==0)
                return o1.compareTo(o2);
            return d;
        });
        List<String> list = new ArrayList<>();
        long limit1 = limit;
        for (String key : toSort) {
            if (limit1-- == 0) break;
            list.add(key);
        }
        return list;
    }
}
