package com.epam.cdp.m2.hw2.aggregator;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javafx.util.Pair;
import lombok.NonNull;

import static java.util.stream.Collectors.*;
public class Java7Aggregator implements Aggregator {

    @Override
    public int sum(@NonNull List<Integer> numbers) {
        int sum = 0;
        for (Integer i : numbers) {
            sum += i;
        }
        return sum;
    }

    @Override
    public List<Pair<String, Long>> getMostFrequentWords(@NonNull List<String> words, long limit) {
        Map<String, Long> map = new HashMap<>();
        for (String s : words) {
            String toUpperCase = s.toUpperCase(Locale.ROOT);
            map.merge(toUpperCase, 1L, Long::sum);
        }
        List<Pair<String, Long>> toSort = new ArrayList<>();
        for (Map.Entry<String, Long> p : map
                .entrySet()) {
            Pair<String, Long> stringLongPair = new Pair<>(p.getKey().toLowerCase(Locale.ROOT), p.getValue());
            toSort.add(stringLongPair);
        }
        toSort.sort((o1, o2) -> (int) (o2.getValue() - o1.getValue()));
        List<Pair<String, Long>> list = new ArrayList<>();
        long limit1 = limit;
        for (Pair<String, Long> stringLongPair : toSort) {
            if (limit1-- == 0) break;
            list.add(stringLongPair);
        }
        return list;
    }

    @Override
    public List<String> getDuplicates(@NonNull List<String> words, long limit) {
        List<String> toSort = new ArrayList<>();
        Map<String, Long> map = new HashMap<>();
        for (String s : words) {
            String toUpperCase = s.toUpperCase(Locale.ROOT);
            map.merge(toUpperCase, 1L, Long::sum);
        }
        for (Map.Entry<String, Long> p : map
                .entrySet()) {
            if (p.getValue() > 1) {
                String key = p.getKey();
                toSort.add(key);
            }
        }
        toSort.sort((o1, o2) -> o1.length() - o2.length());
        List<String> list = new ArrayList<>();
        long limit1 = limit;
        for (String key : toSort) {
            if (limit1-- == 0) break;
            list.add(key);
        }
        return list;
    }
}
