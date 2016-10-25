(ns org.corfudb.shell)

(import org.corfudb.runtime.CorfuRuntime)
(import org.corfudb.runtime.clients.NettyClientRouter)
(import org.docopt.Docopt)
(use 'clojure.reflect)

(defn -class-starts-with [obj name] (if (nil? obj) false (.. (.. (.. obj (getClass)) (getName)) (startsWith name))))
(def -special-classes (list "org.corfudb.runtime.CorfuRuntime" "org.corfudb.runtime.clients"))
(defn -nice-param-list [x] (clojure.string/join "," x))
(defn -print-java-detail [x]
  (clojure.string/join "\n" (->> x reflect :members
       (filter :return-type)
       (filter (fn [x] (filter :public (:flags x))))
       (filter (fn [x] (not (.. (str (:name x)) (contains "$")))))
       (map (fn [x] (format "%s(%s)"
            (:name x)
            (-nice-param-list (:parameter-types x)))))
       )))

(defn -value-fn [val] (if (nil? val) "nil" (if (some true? (map (partial -class-starts-with val) -special-classes))
                                 (-print-java-detail val) (.. val (toString)))))

; Help function
(defn help ([])
  ([object] (println (-print-java-detail object))))

; Runtime and router variables
(def *r nil)
(def *o nil)
(def *args nil)
(def usage "The Corfu Shell.
Usage:
  shell [-n] [-s <script>] [<args>...]
Options:
  -s <script>    Clojure script to run in the shell.
  -n --no-exit   When used with a script, does not terminate automatically.
  -h, --help     Show this screen.
")

(defn -formify-file-base [f] (str "(do " (slurp f) ")"))

(defn -formify-file-exit [f, noexit] (if noexit (do (println "exit") f)
                                         (str "(do " f " (Thread/sleep 1) (System/exit -1))")))

(defn -formify-file [f, noexit]
  (read-string (-formify-file-exit (-formify-file-base f) noexit)))



(defn -main [& args]
  (def cmd (.. (new Docopt usage) (parse (if (nil? args) (make-array String 0) args))))
  (require 'reply.main)
  (def *args (.. cmd (get "<args>")))
  (def repl-args {:custom-eval       '(do (println "Welcome to the Corfu Shell")
                                       (println "This shell is running on Clojure" (clojure-version))
                                       (println "All Clojure commands are valid commands in this shell.")
                                       (println
"Commands you might find helpful:
 (get-runtime \"<connection-string>\") - Obtains a CorfuRuntime.
 (get-router \"<endpoint-name>\") - Obtains a router, to interact with a Corfu server.
 (help [<object>]) - Get the names of methods you can invoke on objects,
                     Or just gives general help if no arguments are passed.
 (quit) - Exits the shell.

The special variables *1, *2 and *3 hold results of commands, and *e holds the last exception.
The variable *r holds the last runtime obtrained, and *o holds the last router obtained.
")
                                       (in-ns 'org.corfudb.shell))
                  :color true
                  :caught '(do (System/exit 0))
                  :skip-default-init true})
  (if (.. cmd (get "-s")) (def repl-args (assoc repl-args :custom-eval '(do (in-ns 'org.corfudb.shell)))) ())
  (if (.. cmd (get "-s")) (def repl-args (assoc repl-args :custom-init
     (org.corfudb.shell/-formify-file (.. cmd (get "-s")) (.. cmd (get "--no-exit"))))) ())
  ((ns-resolve 'reply.main 'launch-nrepl) repl-args) (System/exit 0))


; Util functions to get a host or port from an endpoint string.
(defn get-port [endpoint] (Integer/parseInt (get (.. endpoint (split ":")) 1)))
(defn get-host [endpoint] (get (.. endpoint (split ":")) 0))

; Get a runtime or a router, and add it to the runtime/router table
(defn add-client ([client] (.. *o (addClient client)))
  ([client, router] (.. router (addClient client))))
(defn get-runtime [endpoint] (def *r (new CorfuRuntime endpoint)) *r)
(defn get-router [endpoint]  (do
   (def *o (new NettyClientRouter (get-host endpoint) (get-port endpoint))))
   (add-client (new org.corfudb.runtime.clients.LayoutClient))
   (add-client (new org.corfudb.runtime.clients.LogUnitClient))
   (add-client (new org.corfudb.runtime.clients.SequencerClient))
  *o)


; Functions that get clients from a router
(defn get-base-client ([] (.. *o (getClient org.corfudb.runtime.clients.BaseClient)))
  ([router] (.. router (getClient org.corfudb.runtime.clients.BaseClient))))
(defn get-layout-client ([] (.. *o (getClient org.corfudb.runtime.clients.LayoutClient)))
  ([router] (.. router (getClient org.corfudb.runtime.clients.LayoutClient))))
(defn get-logunit-client ([] (.. *o (getClient org.corfudb.runtime.clients.LogUnitClient)))
  ([router] (.. router (getClient org.corfudb.runtime.clients.LogUnitClient))))
(defn get-sequencer-client ([] (.. *o (getClient org.corfudb.runtime.clients.SequencerClient)))
  ([router] (.. router (getClient org.corfudb.runtime.clients.SequencerClient))))