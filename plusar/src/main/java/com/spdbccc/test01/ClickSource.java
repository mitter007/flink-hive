package com.spdbccc.test01;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {

    private Boolean running = true;
    
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {

        Random random = new Random();
        String[] users = {"Songsong", "Bingbing"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running) {
            sourceContext.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
        }
    }
    
    @Override
    public void cancel() {
        running = false;
    }
}
