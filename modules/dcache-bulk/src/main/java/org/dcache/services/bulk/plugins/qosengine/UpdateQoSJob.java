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
package org.dcache.services.bulk.plugins.qosengine;

import diskCacheV111.util.PnfsId;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.qos.QoSException;
import org.dcache.qos.data.FileQoSRequirements;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.remote.clients.RemoteQoSRequirementsClient;
import org.dcache.services.bulk.QoSEngineAware;
import org.dcache.services.bulk.job.BulkJobKey;
import org.dcache.services.bulk.job.SingleTargetJob;
import org.dcache.services.bulk.util.QoSResponseReceiver;
import org.dcache.vehicles.qos.QoSTransitionCompletedMessage;

import static org.dcache.services.bulk.plugins.qosengine.UpdateQoSJobProvider.TARGET_QOS;

/**
 *  Transition as single file from its current QoS to a new one.
 */
public class UpdateQoSJob extends SingleTargetJob implements QoSEngineAware {
  private QoSResponseReceiver responseReceiver;
  private CellStub qosEngine;

  public UpdateQoSJob(BulkJobKey key, BulkJobKey parentKey, String activity) {
    super(key, parentKey, activity);
  }

  @Override
  public synchronized boolean cancel() {
    RemoteQoSRequirementsClient client = new RemoteQoSRequirementsClient();
    client.setRequirementsService(qosEngine);
    PnfsId pnfsId = attributes.getPnfsId();
    try {
      client.fileQoSRequirementsModifiedCancelled(pnfsId);
    } catch (QoSException e) {
      LOGGER.error("fileQoSRequirementsModifiedCancelled failed: {}, {}.", pnfsId, e.getMessage());
    }
    return super.cancel();
  }

  @Override
  public void relay(QoSTransitionCompletedMessage message) {
    PnfsId pnsfId = message.getPnfsId();
    QoSAction action = message.getAction();
    Object error = message.getErrorObject();
    if (error != null) {
      String errorMessage;
      if (error instanceof Throwable) {
        errorMessage = ((Throwable) error).getMessage();
      } else {
        errorMessage = error.toString();
      }
      LOGGER.error("QoS migration failed: {}, {}, {}.", pnsfId, action, errorMessage);
      setError(error);
    } else {
      LOGGER.debug("QoS migration completed: {}, {}.", pnsfId, action);
      setState(State.COMPLETED);
    }
  }

  @Override
  protected void doRun() {
    if (arguments == null) {
      setError(new IllegalArgumentException("no target qos given."));
      return;
    }

    FileQoSRequirements requirements = new FileQoSRequirements(attributes.getPnfsId(), attributes);

    String targetQos = arguments.get(TARGET_QOS.getName());
    if (targetQos == null) {
      setError(new IllegalArgumentException("no target qos given."));
      return;
    }

    if (targetQos.contains("disk")) {
      requirements.setRequiredDisk(1);
    }

    if (targetQos.contains("tape")) {
      requirements.setRequiredTape(1);
    }

    responseReceiver.register(attributes.getPnfsId().toString(), this);

    RemoteQoSRequirementsClient client = new RemoteQoSRequirementsClient();
    client.setRequirementsService(qosEngine);
    client.fileQoSRequirementsModified(requirements);

    /*
     *  Moving too quickly into the WAITING state can cause unfair starvation of
     *  later-arriving requests.
     *
     *  Let's be a good citizen and consider ourselves RUNNING for at least a second (unless
     *  the job completes first).
     */
    try {
      synchronized (this) {
        wait(TimeUnit.SECONDS.toMillis(1));
      }
    } catch (InterruptedException e) {
      setError(e);
      return;
    }

    setState(State.WAITING);
  }

  @Override
  public void setQoSEngine(CellStub qosEngine) {
    this.qosEngine = qosEngine;
  }

  @Override
  public void setQoSResponseReceiver(QoSResponseReceiver responseReceiver) {
    this.responseReceiver = responseReceiver;
  }
}
