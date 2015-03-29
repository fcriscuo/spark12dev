package org.mskcc.cbio.spark12dev.examples;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Copyright (c) 2015 Memorial Sloan-Kettering Cancer Center.
 * <p>
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY, WITHOUT EVEN THE IMPLIED WARRANTY OF
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.  The software and
 * documentation provided hereunder is on an "as is" basis, and
 * Memorial Sloan-Kettering Cancer Center
 * has no obligations to provide maintenance, support,
 * updates, enhancements or modifications.  In no event shall
 * Memorial Sloan-Kettering Cancer Center
 * be liable to any party for direct, indirect, special,
 * incidental or consequential damages, including lost profits, arising
 * out of the use of this software and its documentation, even if
 * Memorial Sloan-Kettering Cancer Center
 * has been advised of the possibility of such damage.
 * <p>
 * Created by Fred Criscuolo on 3/28/15.
 * criscuof@mskcc.org
 */
public class JavaGeneCounter {
    private static final String cosmicFileName = "/data/cosmic/CosmicMutantExport.tsv";
    private static final Splitter tabSplitter = Splitter.on("\t");
    public static void main(String...args)
    {
        TreeMap<String, Integer> frequencyData = new TreeMap<String, Integer>( );
        Stopwatch stopwatch = Stopwatch.createStarted();
        readWordFile(frequencyData);
        stopwatch.stop();
        printAllCounts(frequencyData);
        System.out.println("Elapsed time is " + stopwatch.elapsed(TimeUnit.MILLISECONDS) +" milliseconds");
    }

    public static int getCount
            (String word, TreeMap<String, Integer> frequencyData)
    {
        return (frequencyData.containsKey(word))?frequencyData.get(word)
                :0;
    }

    public static void printAllCounts(TreeMap<String, Integer> frequencyData)
    {
        for(String word : frequencyData.keySet( ))
        {
            System.out.printf("%15d    %s\n", frequencyData.get(word), word);
        }
    }

    public static void readWordFile(TreeMap<String, Integer> frequencyData)
    {
        Scanner wordFile;
        String gene;
        Integer count;   // The number of occurrences of the gene
        try (BufferedReader reader = new BufferedReader(new FileReader(cosmicFileName)))
        {

            while( (gene = tabSplitter.splitToList(reader.readLine()).get(0)) != null) {
                count = getCount(gene, frequencyData) + 1;
                frequencyData.put(gene, count);
            }
        }
        catch (Exception e)
        {
            System.err.println(e);
            return;
        }

    }
}
