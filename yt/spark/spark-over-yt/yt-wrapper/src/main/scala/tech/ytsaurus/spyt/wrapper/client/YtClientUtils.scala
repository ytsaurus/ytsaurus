package tech.ytsaurus.spyt.wrapper.client

import io.netty.channel.MultithreadEventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.unix.DomainSocketAddress
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
import tech.ytsaurus.spyt.wrapper.system.SystemUtils
import tech.ytsaurus.client.{CompoundClient, DirectYTsaurusClient, DiscoveryMethod, YTsaurusClient, YTsaurusClientConfig, YTsaurusCluster}
import tech.ytsaurus.client.bus.DefaultBusConnector
import tech.ytsaurus.client.rpc.{RpcOptions, YTsaurusClientAuth}

import java.net.SocketAddress
import java.util.concurrent.ThreadFactory
import java.util.{ArrayList => JArrayList}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Failure, Success}

trait YtClientUtils {
  private val log = LoggerFactory.getLogger(getClass)

  private val daemonThreadFactory = new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread
    }
  }

  def createRpcClient(id: String, config: YtClientConfiguration): YtRpcClient = {
    log.info(s"Create RPC YT Client, id $id, configuration ${config.copy(token = "*****")}")

    jobProxyEndpoint match {
      case Some(jobProxy) =>
        log.info(s"Create job proxy client with config $jobProxy")
        createYtClientWrapper(id, config.timeout, new EpollEventLoopGroup(1, daemonThreadFactory)) {
          case (connector, options) => createJobProxyClient(config, connector, options, jobProxy)
        }
      case None =>
        createYtClientWrapper(id, config.timeout, new NioEventLoopGroup(1, daemonThreadFactory)) {
          case (connector, options) => createYtClient(config, connector, options)
        }
    }
  }

  private def createYtClientWrapper(id: String, timeout: Duration, group: MultithreadEventLoopGroup)
                            (client: (DefaultBusConnector, RpcOptions) => CompoundClient): YtRpcClient = {
    val connector = new DefaultBusConnector(group, true)
      .setReadTimeout(toJavaDuration(timeout))
      .setWriteTimeout(toJavaDuration(timeout))

    try {
      val rpcOptions = new RpcOptions()
      rpcOptions.setTimeouts(timeout)

      val yt = client(connector, rpcOptions)
      log.info(s"YtClient $id created")
      YtRpcClient(id, yt, connector)
    } catch {
      case e: Throwable =>
        connector.close()
        throw e
    }
  }

  private def createYtClient(config: YtClientConfiguration,
                             connector: DefaultBusConnector,
                             rpcOptions: RpcOptions): CompoundClient = {
    byopLocalEndpoint(config) match {
      case Some(byop) =>
        log.info(s"Create local BYOP client with config $byop")
        createByopLocalProxyClient(connector, rpcOptions, config, byop)
      case None =>
        byopRemoteEndpoint(config) match {
          case Some(byopRemote) =>
            log.info(s"Create remote BYOP client with config $byopRemote")
            createByopRemoteProxiesClient(connector, rpcOptions, config, byopRemote)
          case None =>
            log.info(s"Create remote proxies client")
            createRemoteProxiesClient(connector, rpcOptions, config)
        }
    }
  }

  private def byopLocalEndpoint(config: YtClientConfiguration): Option[HostAndPort] = {
    if (config.byop.enabled && SystemUtils.isEnabled("byop")) {
      for {
        host <- SystemUtils.envGet("byop_host")
        port <- SystemUtils.envGet("byop_port").map(_.toInt)
      } yield HostAndPort(host, port)
    } else None
  }

  private def jobProxyEndpoint: Option[SocketAddress] = {
    if (SystemUtils.isEnabled("rpc_job_proxy")) {
      for {
        socketFile <- sys.env.get("YT_JOB_PROXY_SOCKET_PATH")
      } yield new DomainSocketAddress(socketFile)
    } else {
      log.info("RPC Job proxy disabled by env variable")
      None
    }
  }

  private def byopRemoteEndpoint(config: YtClientConfiguration): Option[HostAndPort] = {
    if (config.byop.remote.enabled) {
      config.masterWrapperUrl.map(HostAndPort.fromString) match {
        case Some(url) =>
          val client = new MasterWrapperClient(url)
          client.byopEnabled match {
            case Success(true) =>
              log.info("BYOP enabled in cluster")
              client.discoverProxies match {
                case Success(Nil) | Failure(_) =>
                  log.info(s"Workers list is empty, use strategy ${config.byop.remote.emptyWorkersListStrategy}")
                  config.byop.remote.emptyWorkersListStrategy.use(client)
                case Success(_) =>
                  log.info("Workers list non empty")
                  Some(url)
              }
            case Success(false) =>
              log.info("BYOP disabled in cluster")
              None
            case Failure(exception) =>
              log.warn(s"Unexpected error in BYOP Discovery: ${exception.getMessage}")
              None
          }
        case None =>
          log.info("BYOP enabled, but BYOP Discovery URL is not provided")
          None
      }
    } else None
  }

  private def createByopLocalProxyClient(connector: DefaultBusConnector,
                                         rpcOptions: RpcOptions,
                                         config: YtClientConfiguration,
                                         byopEndpoint: HostAndPort): SingleProxyYtClient = {
    new SingleProxyYtClient(
      connector,
      config.clientAuth,
      rpcOptions,
      HostAndPort.fromString(byopEndpoint.toString)
    )
  }

  private def buildYTsaurusClient(connector: DefaultBusConnector, cluster: YTsaurusCluster,
                                  clientAuth: YTsaurusClientAuth, rpcOptions: RpcOptions): YTsaurusClient = {
    YTsaurusClient.builder()
      .setSharedBusConnector(connector)
      .setClusters(java.util.List.of[YTsaurusCluster](cluster))
      .setAuth(clientAuth)
      .setRpcOptions(rpcOptions)
      .build()
  }

  private def createJobProxyClient(config: YtClientConfiguration,
                                   connector: DefaultBusConnector,
                                   rpcOptions: RpcOptions,
                                   address: SocketAddress): DirectYTsaurusClient = {
    DirectYTsaurusClient.builder()
      .setSharedBusConnector(connector)
      .setAddress(address)
      .setAuth(config.clientAuth)
      .setConfig(YTsaurusClientConfig.builder().setRpcOptions(rpcOptions).build())
      .build()
  }

  private def createRemoteProxiesClient(connector: DefaultBusConnector,
                                        rpcOptions: RpcOptions,
                                        config: YtClientConfiguration): YTsaurusClient = {
    val cluster = new YTsaurusCluster(
      config.fullProxy,
      config.fullProxy,
      config.port,
      new JArrayList(),
      config.proxyRole.orNull)

    val client = buildYTsaurusClient(connector, cluster, config.clientAuth, rpcOptions)

    initYtClient(client)
  }

  private def initYtClient(client: YTsaurusClient): YTsaurusClient = {
    try {
      client.waitProxies.join
      client
    } catch {
      case e: Throwable =>
        client.close()
        throw e
    }
  }

  private def createByopRemoteProxiesClient(connector: DefaultBusConnector,
                                            rpcOptions: RpcOptions,
                                            config: YtClientConfiguration,
                                            byopDiscoveryEndpoint: HostAndPort): YTsaurusClient = {
    val cluster = new YTsaurusCluster(
      s"${config.fullProxy}-byop",
      byopDiscoveryEndpoint.host,
      byopDiscoveryEndpoint.port
    )

    rpcOptions.setPreferableDiscoveryMethod(DiscoveryMethod.HTTP)

    val client = buildYTsaurusClient(connector, cluster, config.clientAuth, rpcOptions)

    initYtClient(client)
  }

  implicit class RichRpcOptions(options: RpcOptions) {
    def setTimeouts(timeout: Duration): RpcOptions = {
      options.setGlobalTimeout(toJavaDuration(timeout))
      options.setStreamingReadTimeout(toJavaDuration(timeout))
      options.setStreamingWriteTimeout(toJavaDuration(timeout))
    }
  }

}
