package com.xebia.flink.workshop.optimisations.reinterpret;

import com.xebia.flink.workshop.optimisations.reinterpret.model.Event;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

@RequiredArgsConstructor
class CountLettersAndDigits implements MapFunction<Event, Event> {

    @FunctionalInterface
    public interface StringProvider extends Serializable {
        String get(Event event);
    }

    @FunctionalInterface
    public interface LetterCountSetter extends Serializable {
        void set(Event event, int count);
    }

    @FunctionalInterface
    public interface DigitCountSetter extends Serializable {
        void set(Event event, int count);
    }

    private final StringProvider stringProvider;
    private final LetterCountSetter letterCountSetter;
    private final DigitCountSetter digitCountSetter;

    @Override
    public Event map(Event event) throws Exception {
        int letterCount = 0;
        int digitCount = 0;

        String stringValue = stringProvider.get(event);

        for (int i = 0; i < stringValue.length(); i++) {
            char ch = stringValue.charAt(i);

            if (Character.isLetter(ch)) {
                letterCount++;
            } else if (Character.isDigit(ch)) {
                digitCount++;
            }
        }

        letterCountSetter.set(event, letterCount);
        digitCountSetter.set(event, digitCount);
        return event;
    }
}
