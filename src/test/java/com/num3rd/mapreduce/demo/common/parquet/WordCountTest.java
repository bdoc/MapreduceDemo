package com.num3rd.mapreduce.demo.common.parquet;

import org.junit.Test;

/**
 * Created by pipe on 3/16/17.
 */
public class WordCountTest {
    @Test
    public void main() throws Exception {
        String HOME = System.getenv("HOME");
        String inDir = HOME + "/input";
        String outDir = HOME + "/output/" + System.currentTimeMillis();
        String[] args = new String[2];
        args[0] = inDir;
        args[1] = outDir;
        WordCount.main(args);
    }

}