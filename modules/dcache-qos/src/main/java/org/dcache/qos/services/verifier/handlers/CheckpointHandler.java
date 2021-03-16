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
package org.dcache.qos.services.verifier.handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.services.verifier.data.FileQoSOpCheckpointRecord;
import org.dcache.qos.services.verifier.data.FileQoSOperation;
import org.dcache.qos.services.verifier.data.FileQoSOperationMap;
import org.dcache.qos.services.verifier.data.PoolInfoMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  This implementation writes out the toString of the wrapper record to a text file.
 */
public final class CheckpointHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(CheckpointHandler.class);

    private PoolInfoMap poolInfoMap;
    private FileQoSOperationMap fileQoSOperationMap;

    /**
     *  Read back in from the checkpoint file operation records. These are converted
     *  to {@link FileQoSUpdate} objects and passed to the map for registration.
     *  <p/>
     *  The file to be reloaded is renamed, so that any checkpointing begun while
     *  the reload is in progress does not overwrite the file. In the case of a failed
     *  reload, the reload file should be manually merged into the current checkpoint
     *  file before restart.
     *
     *  @param checkpointFilePath to read
     */
    public void load(String checkpointFilePath) {
        if (!new File(checkpointFilePath).exists()) {
            return;
        }

        File current = new File(checkpointFilePath);
        File reload = new File(checkpointFilePath + "-reload");
        current.renameTo(reload);

        try (BufferedReader fr = new BufferedReader(new FileReader(reload))) {
            while (true) {
                String line = fr.readLine();
                if (line == null) {
                    break;
                }
                try {
                    fileQoSOperationMap.register(new FileQoSOpCheckpointRecord(line).toUpdate());
                } catch (QoSException e) {
                    LOGGER.warn("{}; skipping record.", e.getMessage());
                }
            }
            reload.delete();
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to reload checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during reload checkpoint file: {}",
                            e.getMessage());
        }
    }

    /**
     *  Since we use checkpointing as an approximation, the fact that the ConcurrentMap
     *  (internal to the {@link FileQoSOperationMap) may be dirty and that it is not
     *  locked should not matter greatly.
     *
     *  @param checkpointFilePath where to write.
     *  @param iterator from a ConcurrentHashMap implementation of the operation map index.
     *  @return number of records written
     */
    public long save(String checkpointFilePath, Iterator<FileQoSOperation> iterator) {
        File current = new File(checkpointFilePath);
        File old = new File(checkpointFilePath + "-old");
        if (current.exists()) {
            current.renameTo(old);
        }

        AtomicLong count = new AtomicLong(0);
        try (PrintWriter fw = new PrintWriter(new FileWriter(checkpointFilePath, false))) {
            while (iterator.hasNext()) {
                FileQoSOperation operation = iterator.next();
                fw.println(new FileQoSOpCheckpointRecord(operation, poolInfoMap).toString());
                count.incrementAndGet();
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("Unable to save checkpoint file: {}", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("Unrecoverable error during save of checkpoint file: {}",
                            e.getMessage());
        }

        return count.get();
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setFileQoSOperationMap(FileQoSOperationMap fileQoSOperationMap) {
        this.fileQoSOperationMap = fileQoSOperationMap;
    }
}
