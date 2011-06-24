/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */
package org.dcache.web;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import javax.servlet.ServletException;

/**
 *
 */

/**
 * @author V. Podstavkov
 *
 */
public class EpsPlotBuilder implements PlotBuilder {

    /* (non-Javadoc)
     * @see PlotBuilder#buildPlot(java.lang.String, Plot, java.lang.Object)
     */

    /**
     * Creates plot for several datasets with individual Xs,Ys and puts it into the file
     * @param filename
     * @param styles
     * @param gnuSetup
     * @throws ServletException
     * @throws IOException
     */
    public void buildPlot(String filename, Plot plot, Object hlp)
    throws IOException
    {
        Process p = Runtime.getRuntime().exec("gnuplot");
        PrintWriter stdOutput = new PrintWriter(new BufferedWriter(new OutputStreamWriter(p.getOutputStream())));

        if (hlp instanceof GnuSetup) {
            GnuSetup gnuSetup = (GnuSetup)hlp;                                      // Get the setup object

            for (int i = 0; i < gnuSetup.size(); i++) {                             // Walk over the command list
                stdOutput.println(gnuSetup.get(i));                                 // Pass the command to gnuplot
            }
            int nstyles = gnuSetup.getStylesSize();                                 // Get the number of datasets
            // Loop over the datasets
            // Here we have to exclude the empty datasets, otherwise gnuplot does not build the plot
            for (int i = 0; i < nstyles; i++) {                                     // Walk over the style definitions
                plot.addDataSet(gnuSetup.getDataSrcName(i));                        // Very important! Otherwise plot "will not know" how many datasets were plotted
                DataSrc dsrc = plot.getDataSrc(i);
                if (dsrc.getNumberOfRows() == 0) continue;                          // Skip empty dataset
                if (dsrc == null) {
                    System.err.printf("Error in xml plot configuration file: '%s' datasource does not exist", gnuSetup.getDataSrcName(i));
                    continue;
                }
                dsrc.addTitle(gnuSetup.getDataSetTitle(i));
                String dsString = this.dataSetString(plot, i, gnuSetup.getDataStyle(i)); // Prepare the actual gnuplot style string using the line with column names
                stdOutput.print((i==0 ? "plot " : ", ") + dsString);                // Construct 'plot' command
            }

            stdOutput.println();                                                    // Send and execute the command
            stdOutput.println("quit");                                              // Quit gnuplot
            stdOutput.close(); stdOutput = null;                                    // Close the command pipe
            try {
                p.waitFor();                                                        // Wait until gnuplot is done
            }
            catch (InterruptedException x) {
                System.out.println("Exception happened - here's what I know: ");
                x.printStackTrace();
            }

            System.out.println("File "+filename+".eps is ready");

            createPng(filename);
            createPreview(filename);
        } else {
            throw new IOException("Wrong gnuplot setup");
        }
    }

    private String dataSetString(Plot plot, int num, String style) {
        String filename = plot.getDataSrcFilename(num);                 // Get the dataset filename
        String[] aa = style.split("[{}]");                              // Split the gnuplot data string to find any {xxx} inside
        StringBuffer rr = new StringBuffer();
        System.err.print("Add to selected: ");  // Debug
        for (String a : aa) {                                           // Walk over the splitted string
            Integer n = plot.getDSColNumber(num, a);                    // Try to get column number with such name
            if (n != null && n != 1) {                                  // We don't want to add the Xs to selected columns, but this has to be smarter than that
                plot.getDataSrc(num).addSelected(n);
                System.err.print(n+" ");        // Debug
            }
            rr.append((n==null) ? a : n);                               // If found put the culumn number instead of name, if not pass the name as is
        }
        System.err.println();                   // Debug
        String reply = "'"+filename+"' " + rr;                          // Construct the whole string
        System.err.println("Rebuilt string="+reply);                    // Print for debug
        return reply;                                                   // Return the result
    }

    private void createPng(final String filename)
    throws IOException
    {
        try {
            Process pc = Runtime.getRuntime().exec("convert -depth 8 -density 100x100 -modulate 95,95 "+filename+".eps "+filename+".png");

             try {
                pc.waitFor();
            }
            catch (InterruptedException x) {}
        }
        catch (IOException ex) {
            System.out.println("exception happened during 'convert' - here's what I know: ");
            ex.printStackTrace();
        }
    }

    private void createPreview(final String filename)
    throws IOException
    {
        try {
//          Process pc = Runtime.getRuntime().exec("convert -geometry 200x200 -modulate 90,50 "+filename+".eps png:"+filename+".pre");
            Process pc = Runtime.getRuntime().exec("convert -depth 8 -geometry 200x200 -modulate 95,95 "+filename+".eps png:"+filename+".pre");

             try {
                pc.waitFor();
            }
            catch (InterruptedException x) {}
        }
        catch (IOException ex) {
            System.out.println("exception happened during 'convert' - here's what I know: ");
            ex.printStackTrace();
        }
    }
}
