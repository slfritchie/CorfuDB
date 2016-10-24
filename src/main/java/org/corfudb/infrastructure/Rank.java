package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * Tuple to store the rank and clientId for each round in Paxos.
 * Created by mdhawan on 6/28/16.
 */
@Slf4j
@ToString
@AllArgsConstructor
public class Rank implements Comparable<Rank> {
    @Getter
    Long rank;
    @Getter
    UUID clientId;

    /**
     * compares this.rank with other.rank
     * if equal
     * compares this.clientId with other.clientId
     *
     * @param other
     * @return
     */
    @Override
    public int compareTo(Rank other) {
        /*
        ** I spent a fair amount of time trying to figure out what prepare
        ** with a duplicate rank would sometime succeed.  Here is why.
        ** TODO: uncomment this code once its purpose is understood.
        ** The LayoutServerTest.checkPhase1AndPhase2MessagesFromMultipleClients test is broken until this hack is reverted.
        **
        ** OK, I now understand what's happening here.  The commented code below
        ** is acting as a tie-breaker that traditional Paxos doesn't have.  Paxos
        ** relies the uniqueness of the rank integer alone, which means that each
        ** client needs to use only a subset of all possible ranks, e.g. reserve some
        ** lower bits for a client-ID-like-unique-across-all-participants integer.
        **
        ** This hidden comparison to the clientId string does NOT play nicely with
        ** QuickCheck: with the clientId hidden, QC has no way to tell why a server
        ** decided to compare rank (rank = integer only) in crazy and
        ** nondeterministic ways.

        if (rank.compareTo(other.getRank()) == 0) {
            return clientId.compareTo(other.clientId);
        }
        */
        return rank.compareTo(other.getRank());
    }
}
