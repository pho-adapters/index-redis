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

use Pho\Kernel\Kernel;
use Pho\Kernel\Services\ServiceInterface;
use Pho\Kernel\Services\Index\IndexInterface;
use Pho\Lib\Graph\EntityInterface;
use Redis\Graph;
use Redis\Graph\Node;
use Redis\Graph\Edge;

/**
 * Redis indexing adapter
 * 
 * Redis Graph is a Redis module that makes it 
 * Cypher queriable.
 *
 * @author Emre Sokullu
 */
class Redis implements IndexInterface, ServiceInterface
{

     /**
     * Pho-kernel 
     * 
     * @var Kernel
     */
    protected $kernel;

    /**
     * Redis Client
     * 
     * @var Predis\Client
     */
    protected $client;


    /**
     * Constructor.
     * 
     * Initializes RedisGraph connection. 
     * Runs indexing on kernel signals.
     * 
     * @param Kernel $kernel Pho Kernel
     * @param string $uri Connection details for the Redis server
     */
    public function __construct(Kernel $kernel, string $uri = "")
    {
        $this->kernel = $kernel;
    
        $this->client = new Graph('index', $this->kernel->database()->client());
        
        $this->subscribeGraphsystem();
    }

    /**
     * Listener for kernel events
     * 
     * Interfaces graphsystem and indexes
     * in every touch or delete operation.
     * 
     * @return void
     */
    protected function subscribeGraphsystem(): void
    {
        $this->kernel->events()->on('graphsystem.touched',  
            function(array $var) {
                $this->index($var);
            })
            ->on('graphsystem.node_deleted',  
            function(string $id) {
                $this->nodeDeleted($id);
            })
            ->on('graphsystem.edge_deleted',  
            function(string $id) {
                $this->edgeDeleted($id);
            }
        );
    }

    private static function escape(/*mixed*/ $param): string
        {
            return (string) (( is_int($param) || is_double($param) ) ? $param : ('"' . addslashes($param) . '"' ));
        }

    /**
     * {@inheritDoc}
     */
    public function query(string $query, array $params = array()): \Pho\Kernel\Services\Index\QueryResult
    {
        foreach($params as $key=>$param) {
            $query = str_replace("{".$key."}", self::escape($param), $query);
        }
        error_log("query is: ".$query);
        $result = $this->client->query($query);
        //eval(\Psy\sh());
        return new QueryResult($result);
    }
 
 
   /**
    * {@inheritDoc}
    */
   public function checkNodeUniqueness(string $field_name, /*mixed*/ $field_value, string $label = ""): bool
   {
       
      if(!empty($label))
        $label = sprintf(":%s", $label);
      $cypher = sprintf(
          "MATCH(n%s {%s: %s}) RETURN n", 
          $label, 
          $field_name, 
          self::escape($field_value)
      );
      $res = $this->client->query($cypher);   
      return (count($res->values) == 0); // that means it's unique
   }

    /**
     * Direct access to the Neo4J client
     * 
     * This class does also provide direct read-only access to the 
     * client, for debugging purposes.
     *
     * @return \GraphAware\Neo4j\Client\Client
     */
    public function client(): \GraphAware\Neo4j\Client\Client
    {
        return $this->client;
    }

    /**
     * {@inheritDoc}
     */
    public function index(array $entity): void 
    {
        $this->kernel->logger()->info("Index request received by %s, a %s.", $entity["id"], $entity["label"]);
        $header = (int) hexdec($entity["id"][0]);
        if($header>0 && $header<6) /// node
        {
            $this->indexNode($entity);
        }
        elseif($header>=6 &&  $header < 11 )  // edge
        {
            $this->indexEdge($entity);
        }
        else {
            throw new \Exception(sprintf("Unrecognized entity type with header %s", $entity["id"][0]));
        }
        $this->kernel->logger()->info("Moving on");
    }

    protected function attributesToCypherCreate(array $entity): string 
    {
            $props = [];
            $attributes = array_merge(["udid"=>$entity["id"]], $entity["attributes"]);
            foreach ($attributes as $key => $val) {
              if (is_int($val) || is_double($val)) {
                $props[] = $key . ':' . $val;
              } else {
                $props[] = $key . ':"' . trim((string)$val, '"') . '"';
              }
            }
            return '{' . implode(',', $props) . '}';      
    }

    protected function attributesToCypherSet(array $entity): string 
    {
            $props = [];
            $attributes = array_merge(["udid"=>$entity["id"]], $entity["attributes"]);
            foreach ($attributes as $key => $val) {
              if (is_int($val) || is_double($val)) {
                $props[] = 'e.' . $key . ' = ' . $val;
              } else {
                $props[] = 'e.' . $key . ' = "' . trim((string)$val, '"') . '"';
              }
            }
            return implode(',', $props);      
    }

    /**
     * Indexes a node
     *
     * @param array $entity In array form.
     * 
     * @return void
     */
    protected function indexNode(array $entity): void
    {
        $query = sprintf("MATCH (n {udid: \"%s\"}) RETURN n", addslashes($entity["id"]));
        $results = $this->client->query($query);
        if(count($results->values)==0) {
            // create
            $query = sprintf("CREATE (:%s %s)", $entity["label"], $this->attributesToCypherCreate($entity));
            error_log("create query is: ".$query);
            $this->client->query($query);
            return;
        }
        // modify
        $query = sprintf("MATCH (e:%s {udid: \"%s\"}) SET %s", $entity["label"], addslashes($entity["id"]), $this->attributesToCypherSet($entity));
        error_log("modify query is: ".$query);
        $this->client->query($query);
    }

    /**
     * Indexes an edge
     *
     * @param array $entity In array form.
     * 
     * @return void
     */
    protected function indexEdge(array $entity): void
    {

        $query = sprintf(
            "MATCH ()-[e: {udid: \"%s\"}]->() DELETE e", 
                addslashes($entity["id"])
        );
        $this->client->query($query);

        $cq = sprintf(
            "MATCH(t {udid: \"%s\"}), (h {udid: \"%s\"}) CREATE (t)-[e:%s %s]->(h)", 
            $entity["tail"],
            $entity["head"],
            $entity["label"],
            $this->attributesToCypherCreate($entity)
        );
        $this->client->query($cq);
    }

    /**
     * {@inheritDoc}
     */
    public function nodeDeleted(string $id): void 
    {
        $this->kernel->logger()->info("Node deletion request received by %s.", $id);

        $cq = sprintf("MATCH (n {udid: \"%s\"})-[e]->() DELETE e", $id);
        $this->client->query($cq);
        $cq = sprintf("MATCH ()-[e]->(n {udid: \"%s\"}) DELETE e", $id);
        $this->client->query($cq);
        $cq = sprintf("MATCH  (n {udid: \"%s\"}) DELETE n", $id);
        $this->client->query($cq);

        $this->kernel->logger()->info("Node deleted. Moving on.");
    }

    /**
     * {@inheritDoc}
     */
    public function edgeDeleted(string $id): void
    {
        $this->kernel->logger()->info("Edge deletion request received by %s.", $id);
        $cq = sprintf("MATCH ()-[e {udid: \"%s\"]->()  DELETE e", $id);
        $this->client->query($cq);
        $this->kernel->logger()->info("Edge deleted. Moving on.");
    }

    /**
     * {@inheritDoc}
     */
    public function flush(): void
    {
        $cq = "MATCH ()-[e]->() DELETE e;";
        $this->client->query($cq);
        $cq = "MATCH  (n) DELETE n;";
        $this->client->query($cq);
    }

    public function createIndex(string $label, string $field_name): void
    {
        return;
       //$cq = sprintf("CREATE INDEX ON :%s(%s)", $label, $field_name);
       //$this->client->run($cq);
    }
 

}
