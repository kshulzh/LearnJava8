package com.epam.cdp.m2.hw2.aggregator;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Collectors;

import javafx.util.Pair;
import lombok.NonNull;

import static java.util.stream.Collectors.*;

public class Java8ParallelAggregator implements Aggregator {

    @Override
    public int sum(List<Integer> numbers) {
        return numbers
                .parallelStream()
                .mapToInt(i->i)
                .sum();
    }

    @Override
    public List<Pair<String, Long>> getMostFrequentWords(@NonNull List<String> words, long limit) {
        return words
                .parallelStream()
                .map(s->s.toUpperCase(Locale.ROOT))
                .collect(groupingBy(Function.identity(),counting()))
                .entrySet()
                .parallelStream()
                .map(p->new Pair<String,Long>(p.getKey().toLowerCase(Locale.ROOT),p.getValue()))
                .sorted((o1, o2) -> (int) (o2.getValue()- o1.getValue()))
                .limit(limit)
                .collect(toList());
    }

    @Override
    public List<String> getDuplicates(@NonNull List<String> words, long limit) {
        return words
                .parallelStream()
                .map(s->s.toUpperCase(Locale.ROOT))
                .collect(Collectors.groupingBy(Function.identity(),Collectors.counting()))
                .entrySet()
                .parallelStream()
                .filter(p->p.getValue()>1)
                .map(p->p.getKey())
                .sorted((o1, o2) -> o1.length()-o2.length())
                .limit(limit)
                .collect(Collectors.toList());
    }
}
