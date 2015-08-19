# Corfu Git Maven Repo

This orphaned branch stores the Corfu maven artifacts.

Please do not commit to this branch directly. It is controlled by the Travis-CI build.

To use this repository in a maven project, add:

```xml
<repositories>
    <repository>
        <id>corfu-github-mvn-repo</id>
        <url>https://raw.github.com/corfudb/corfudb/mvn-repo/</url>
        <snapshots>
            <enabled>true</enabled>
            <updatePolicy>always</updatePolicy>
        </snapshots>
    </repository>
</repositories>
```

Most likely, you will want to add a reference to the Corfu artifact as well:

```xml
<dependencies>
        <dependency>
            <groupId>org.corfudb</groupId>
            <artifactId>corfudb</artifactId>
            <version>0.1-SNAPSHOT</version>
            <scope>compile</scope>
        </dependency>
</dependencies>
```
