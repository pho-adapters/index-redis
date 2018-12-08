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
 * Placeholder.
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
        error_log("Redis QueryResult executing");
         error_log("Resuts is a: ".get_class($results));
        error_log("Resuts are: ".print_r($results, true));
        error_log("Resuts are: ".print_r($results->values, true));
        
/*
       foreach($results->values as $result) // $result would be a \GraphAware\Bolt\Result\Result 
       {
           $this->results[] = array_values($result);
       }
       */
        $this->results = $results->values;
       $stats = $results->stats;
       $this->summary["nodesCreated"] = isset($stats["nodes_created"])?$stats["nodes_created"]:0;
       $this->summary["nodesDeleted"] = isset($stats["nodes_deleted"])?$stats["nodes_deleted"]:0;
       $this->summary["edgesCreated"] = isset($stats["relationships_created"])?$stats["relationships_created"]:0;
       $this->summary["edgesDeleted"] = isset($stats["relationships_deleted"])?$stats["relationships_deleted"]:0;
       $this->summary["propertiesSet"] = isset($stats["properties_set"])?$stats["properties_set"]:0;
       $this->summary["containsUpdates"] = 0;
        
    }
}
