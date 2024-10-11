package tech.ytsaurus.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import java.net.URI;
import java.util.Arrays;

public class StartOperationHelpers {
    private static final Logger logger = LoggerFactory.getLogger(StartOperationHelpers.class);

    // Copied from `BaseLayerDetector.guess_base_layers`
    public static void guessBaseLayers(YTreeNode spec) {
        var userLayer = System.getenv("YT_BASE_LAYER");
        if (userLayer == null) {
            return;
        }
        userLayer = userLayer.toLowerCase().strip();
        if (spec.getAttribute("layer_paths").isPresent() || spec.getAttribute("docker_image").isPresent()) {
            // do not change user spec
            if (Arrays.asList("auto", "porto:auto", "docker:auto").contains(userLayer)) {
                logger.debug("Operation has layer spec. Do not guess base layer");
            }
            return;
        }
        if (Arrays.asList("auto", "docker:auto", "porto:auto").contains(userLayer)) {
            // TODO: implement later
            return;
        }
        if (userLayer.startsWith("//")) {
            var layerPaths = YTree.listBuilder();
            for (var path : userLayer.split(",")) {
                layerPaths.value(path.strip());
            }
            spec.putAttribute("layer_paths", layerPaths.buildList());
        } else if (userLayer.startsWith("registry.")) {
            var path = URI.create("//" + userLayer).getPath();
            if (path != null) {
                spec.putAttribute("docker_image", YTree.stringNode(path.substring(1)));
            }
        } else {
            spec.putAttribute("docker_image", YTree.stringNode(userLayer));
        }
    }
}
