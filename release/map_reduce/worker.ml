open Async.Std

module Make (Job : MapReduce.Job) = struct

  (* see .mli *)
      let rec run r w =
     let module WorkerReq = WorkerRequest(Job) in
     let module WorkerRes = WorkerResponse(Job) in
     WorkerReq.receive r>>= fun req ->
     match req with
     |`Eof -> (try_with (fun () -> return (WorkerRes.send w (WorkerRes.JobFailed("`Eof in worker - receive")))) >>= 
     			fun res -> match res with
     			|Core.Std.Result.Error e -> failwith "writer failed in sending job_failed"
     			|Core.Std.Result.Ok v -> run r w)
     |`Ok(WorkerReq.MapRequest(i)) -> 
                (Job.map i >>= fun res -> try_with (fun () -> return (WorkerRes.send w (WorkerRes.MapResult(res)))) >>=
                 fun res1 -> match res1 with
     			|Core.Std.Result.Error e -> failwith "writer failed in sending MapResult"
     			|Core.Std.Result.Ok v -> run r w)
     |`Ok(WorkerReq.ReduceRequest(k,lst)) -> 
                (Job.reduce (k,lst) >>= fun res -> try_with (fun () -> return (WorkerRes.send w (WorkerRes.ReduceResult(res)))) >>=
                 fun res1 -> match res1 with
     			|Core.Std.Result.Error e -> failwith "writer failed in sending ReduceResult"
     			|Core.Std.Result.Ok v -> run r w)

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


