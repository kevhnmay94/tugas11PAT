package com.test.aja;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by User on 15/12/2015.
 */
public class CobaRegex {
    public static void main( String args[] ){

        // String to be scanned to find the pattern.
        String line = "<article mdate=\"2002-01-03\" key=\"persons/Codd69\">\n" +
                "<author>E. F. Codd</author>\n" +
                "<title>Derivability, Redundancy and Consistency of Relations Stored in Large Data Banks.</title>\n" +
                "<journal>IBM Research Report, San Jose, California</journal>\n" +
                "<volume>RJ599</volume>\n" +
                "<month>August</month>\n" +
                "<year>1969</year>\n" +
                "<cdrom>ibmTR/rj599.pdf</cdrom>\n" +
                "<ee>db/labs/ibm/RJ599.html</ee>\n" +
                "</article>\n" +
                "\n" +
                "<inproceedings key=\"conf/focs/Yao82a\" mdate=\"2011-10-19\">\n" +
                "<title>Theory and Applications of Trapdoor Functions (Extended Abstract)</title>\n" +
                "<author>Andrew Chi-Chih Yao</author>\n" +
                "<pages>80-91</pages>\n" +
                "<crossref>conf/focs/FOCS23</crossref>\n" +
                "<year>1982</year>\n" +
                "<booktitle>FOCS</booktitle>\n" +
                "<url>db/conf/focs/focs82.html#Yao82a</url>\n" +
                "<ee>http://doi.ieeecomputersociety.org/10.1109/SFCS.1982.45</ee>\n" +
                "</inproceedings>";
        String pattern = "<(article|inproceedings|phdthesis|masterthesis)(.*>\\n)*<\\/(article|inproceedings|phdthesis|masterthesis)>";
//        String line = "<author>Craig Gentry</author>\n<author>E. F. Codd</author>";
//        String pattern = "<author>(.*)<\\/author>";

        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);
        int matches = 0;

        // Now create matcher object.
        Matcher m = r.matcher(line);
        while (m.find()) {
            System.out.println(m.group(0));
            System.out.println(m.group(1));
            matches++;
        }
        System.out.println(matches);
    }
}
