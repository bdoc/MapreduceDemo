package com.num3rd.mapreduce.demo.common.compress;

import org.junit.Test;

import static org.junit.Assert.*;

public class TextFile2GzipTest {

    @Test
    public void main() throws Exception {
        String HOME = System.getenv("HOME");
        String inDir = HOME + "/input";
        String outDir = HOME + "/output/" + System.currentTimeMillis();
        String[] args = new String[3];
        args[0] = inDir;
        args[1] = outDir;
        args[2] = "-Dmapreduce.job.queuename=plat";
        TextFile2Gzip.main(args);
    }
}