package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdateManager;

import diskCacheV111.vehicles.Message;
import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;

/**
 * Instances of this class will interpret an incoming reply CellMessages
 * that have instances of GetLinkGroupNamesMessage payload.  It uploads
 * the gathered information into the dCache state.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkgroupListMsgHandler implements MessageHandler {

	private static Logger _log = LoggerFactory.getLogger( LinkgroupListMsgHandler.class);
	private static final StatePath LINKGROUPS_PATH = new StatePath("linkgroups");

	final private StateUpdateManager _sum;

	public LinkgroupListMsgHandler( StateUpdateManager sum) {
		_sum = sum;
	}

	public boolean handleMessage(Message messagePayload, long metricLifetime) {

		if( !(messagePayload instanceof GetLinkGroupNamesMessage))
			return false;

		if( _log.isInfoEnabled())
			_log.info( "received linkgroup list msg.");

		GetLinkGroupNamesMessage msg = (GetLinkGroupNamesMessage) messagePayload;

		String names[] = msg.getLinkGroupNames();

		StateUpdate update = null;

		for( int i = 0; i < names.length; i++) {
			if( update == null)
				update = new StateUpdate();

			if( _log.isDebugEnabled())
				_log.debug("adding linkgroup: " + names[i] + " lifetime: " + metricLifetime);

			update.appendUpdate( LINKGROUPS_PATH.newChild( names[i]), new StateComposite( metricLifetime));
		}

		if( update != null)
			_sum.enqueueUpdate( update);
		else
			_log.info( "received GetLinkGroupNamesMessage with no linkgroups listed");

		return true;
	}

}
