package org.apache.livy.server.interactive

import org.apache.livy.LivyConf
import org.apache.livy.server.AccessManager
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.InteractiveSessionManager
import org.scalatra._
import tech.ytsaurus.spyt.patch.annotations.{OriginClass, Subclass}


@Subclass
@OriginClass("org.apache.livy.server.interactive.InteractiveSessionServlet")
class InteractiveSessionServletForSpyt(
    sessionManager: InteractiveSessionManager,
    sessionStore: SessionStore,
    livyConf: LivyConf,
    accessManager: AccessManager)
  extends InteractiveSessionServlet(sessionManager, sessionStore, livyConf, accessManager) {
  post("/find-by-user") {
    synchronized {
      try {
        val createRequest = bodyAs[CreateInteractiveRequest](request)
        val requestedUser = proxyUser(request, createRequest.proxyUser)
        if (requestedUser.isDefined) {
          val sessionO = sessionManager.all().find(s => s.proxyUser == requestedUser && s.state.isActive)
          sessionO match {
            case Some(session) =>
              session.recordActivity()
              Ok(clientSessionView(session, request),
                headers = Map("Location" -> url(getSession, "id" -> session.id.toString)))
            case None => NotFound(ResponseMessage("Session not found"))
          }
        } else {
          throw new IllegalArgumentException("User is not defined")
        }
      } catch {
        case e: IllegalArgumentException => BadRequest(ResponseMessage("Rejected, Reason: " + e.getMessage))
      }
    }
  }
}
