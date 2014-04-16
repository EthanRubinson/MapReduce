open Async.Std
open Protocol

module Make (Job : MapReduce.Job) = struct

  let rec run r w =
     let module WorkerReq = WorkerRequest(Job) in
     let module WorkerRes = WorkerResponse(Job) in
     WorkerReq.receive r
     >>= fun req ->
      match req with
        | `Eof ->  (Reader.close r)
        | `Ok (WorkerReq.MapRequest(i)) -> 
            (try_with (fun () -> Job.map i) >>= fun res ->
              match res with
     			      |Core.Std.Result.Error e -> (WorkerRes.send w (WorkerRes.JobFailed("Make.run- map"))); run r w
     			      |Core.Std.Result.Ok v ->  (WorkerRes.send w (WorkerRes.MapResult(v))); run r w)
        | `Ok (WorkerReq.ReduceRequest(k,lst)) -> 
            (try_with(fun () -> Job.reduce (k,lst)) >>= fun res ->
              match res with
                |Core.Std.Result.Error e -> (WorkerRes.send w (WorkerRes.JobFailed("Make.run- reduce"))); run r w
                |Core.Std.Result.Ok v ->  (WorkerRes.send w (WorkerRes.ReduceResult(v))); run r w)

end

(* see .mli *)
let init port =
  Tcp.Server.create
    ~on_handler_error:`Raise
    (Tcp.on_port port)
    (fun _ r w ->
      Reader.read_line r >>= function
        | `Eof    -> return ()
        | `Ok job -> match MapReduce.get_job job with
          | None -> return ()
          | Some j ->
            let module Job = (val j) in
            let module Mapper = Make(Job) in
            Mapper.run r w
    )
    >>= fun _ ->
  print_endline "server started";
  print_endline "worker started.";
  print_endline "registered jobs:";
  List.iter print_endline (MapReduce.list_jobs ());
  never ()


