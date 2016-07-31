package com.berkgokden.db;

/**
 * Created by developer on 7/29/16.
 */
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.commons.compress.utils.IOUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 */
public class ElasticsearchServer {

    private final Node node;
    private final String dataDirectoryRoot;


    public ElasticsearchServer() {
        this.dataDirectoryRoot = getClass().getClassLoader()
                .getResource(".").getPath();

        Settings.Builder settings = Settings.settingsBuilder()
                .put("http.enabled", "true")
                .put("path.data", this.dataDirectoryRoot+"data")
                .put("path.home", this.dataDirectoryRoot+"home");
        node = nodeBuilder()
                .settings(settings)
                .node();
    }

    public void start() {
        node.start();
    }

    public void shutdown() {
        node.close();
        deleteDataDirectory();
    }

    private void deleteDataDirectory() {
        try {
            Path rootPath = Paths.get(this.dataDirectoryRoot+"data");
            Files.walk(rootPath, FileVisitOption.FOLLOW_LINKS)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Data directory of elasticsearch server could not be deleted", e);
        }

    }
}
