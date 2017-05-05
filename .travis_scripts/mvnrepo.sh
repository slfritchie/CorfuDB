#!/bin/bash

if [ "$TRAVIS_BRANCH" != "master" ] && [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
##ORIG: if [ "$TRAVIS_BRANCH" == "master" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
    if [ "$TRAVIS_JDK_VERSION" == "oraclejdk8" ]; then
        echo -e "Publishing maven repository..."

        PROJECT_VERSION=$(cat ${HOME}/.project_version)
        mvn -N io.takari:maven:wrapper -Dmaven=3.3.9
        ./mvnw deploy -DskipTests=true
        cp -R target/mvn-repo $HOME/mvn-repo-current
        for pkg in runtime annotations annotationProcessor; do
            cp -R $pkg/target/mvn-repo/* $HOME/mvn-repo-current
            (
                cd $HOME/mvn-repo-current/*/*/$pkg
                SNAPSHOT_DIR=`echo $PROJECT_VERSION | sed s/-.*/-SNAPSHOT/`
                echo DBG: PWD = $PWD
                echo DBG: SNAPSHOT_DIR = $SNAPSHOT_DIR
                rm -rf $SNAPSHOT_DIR
                mkdir $SNAPSHOT_DIR
                cp -pv $PROJECT_VERSION/* $SNAPSHOT_DIR
                (
                    cd $SNAPSHOT_DIR
                    for file in *.jar* *.pom*; do
                        mv -v $i `echo $i | sed 's/-[0-9]*/-SNAPSHOT/'`
                    done
                )
                echo DBG: contents of $SNAPSHOT_DIR
                ls -al .
            )
        done
        cd $HOME
        echo DBG: Exiting now with status 0 ; exit 0
        git config --global user.email "travis@travis-ci.org"
        git config --global user.name "travis-ci"
        git clone --quiet --branch=mvn-repo https://${GH_TOKEN}@github.com/CorfuDB/Corfu-Repos mvn-repo > /dev/null

        cd mvn-repo
        cp -Rf $HOME/mvn-repo-current/* .
        git add -f .
        git commit -m "Updated maven repository from travis build $TRAVIS_BUILD_NUMBER"
        git push -fq origin mvn-repo > /dev/null

        echo -e "Maven artifacts built and pushed to github."
    fi
fi
