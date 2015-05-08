package cmpt741; 

import java.util.*;
import java.lang.*;
import java.io.*;
import org.paukov.combinatorics.*;

class Itemsets 
{
    /*
     *public static void main (String[] args) throws java.lang.Exception
     *{
     *    // your code goes here
     *    String[] basket = {"1","2","3","4"};
     *    ArrayList<String[]> als = GetItemsets(basket, 2);
     *    for(String[] sa: als)
     *    {
     *        for(String s: sa)
     *            System.out.print(s+" ");
     *        System.out.println();
     *    }
     *}
     */

    public ArrayList<String[]> GetItemsets(String[] basket, int k)
    {
        // Create the initial vector
        ICombinatoricsVector<String> initialVector = Factory.createVector(basket);

        // Create a simple combination generator to generate 3-combinations of the initial vector
        Generator<String> gen = Factory.createSimpleCombinationGenerator(initialVector, k);

        // Return all possible combinations
        ArrayList<String[]> Combinations = new ArrayList<String[]>();
        for (ICombinatoricsVector<String> combination : gen) {
            Combinations.add(combination.getVector().toArray(new String[0]));
        }
        return Combinations;
    }


    public ArrayList<String[]> GetItemsetsRecursive(String[] basket, int k)
    {
        ArrayList<String[]> Combinations = new ArrayList<String[]>();
        ArrayList<String> temp = new ArrayList<String>();
        GenerateCombinations(Combinations, basket, 0, k, temp);
        return Combinations;
    }

    public void GenerateCombinations(ArrayList<String[]> Combinations,
            String[] basket, int offset, int k, ArrayList<String> temp)
    {
        if(k==0)
        {
            String[] combo = new String[temp.size()];
            int i = 0;
            for(String s : temp)
                combo[i++] = s;
            Combinations.add(combo);
        }
        else
        {
            for(int i = offset; i <= basket.length-k; i++)
            {
                temp.add(basket[i]);
                GenerateCombinations(Combinations, basket, i+1, k-1, temp);
                temp.remove(temp.size()-1);
            }
        }
    }
}
