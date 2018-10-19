<?php

/*
 * This file is part of the Pho package.
 *
 * (c) Emre Sokullu <emre@phonetworks.org>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Pho\Kernel\Services\Index\Adapters;

/**
 * Neo4j adapter for Kernel's QueryResult class.
 * 
 * @author Emre Sokullu <emre@phonetworks.org>
 */
class QueryResult extends \Pho\Kernel\Services\Index\QueryResult 
{
    /**
     * Constructor
     *
     * @param  $results
     */
     public function __construct($results)
     {
        foreach($results->records() as $result) // $result would be a \GraphAware\Bolt\Result\Result 
        {
            $this->results[] = $result->values()[0]->values();
        }
        $stats = $results->summarize()->updateStatistics();
         if(!is_null($stats)) {
        $this->summary["nodesCreated"] = $stats->nodesCreated();
        $this->summary["nodesDeleted"] = $stats->nodesDeleted();
        $this->summary["edgesCreated"] = $stats->relationshipsCreated();
        $this->summary["edgesDeleted"] = $stats->relationshipsDeleted();
        $this->summary["propertiesSet"] = $stats->propertiesSet();
        $this->summary["containsUpdates"] = $stats->containsUpdates();
         }
         else {
             $this->summary["nodesCreated"] = 0;
            $this->summary["nodesDeleted"] = 0;
            $this->summary["edgesCreated"] = 0;
            $this->summary["edgesDeleted"] = 0;
            $this->summary["propertiesSet"] =0;
            $this->summary["containsUpdates"] = false;
         }
     }
}
