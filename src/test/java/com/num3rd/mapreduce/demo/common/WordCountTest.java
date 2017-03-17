package com.num3rd.mapreduce.demo.common;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by pipe on 3/16/17.
 */
public class WordCountTest {
    @Test
    public void main() throws Exception {
        String inDir = "/tmp/input";
        String outDir = "/tmp/output/" + System.currentTimeMillis();
        String[] args = new String[2];
        args[0] = inDir;
        args[1] = outDir;
        WordCount.main(args);
    }

}