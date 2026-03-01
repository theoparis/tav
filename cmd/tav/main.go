package main

import (
	"fmt"
	"os"
	"strings"

	"tav/internal/tav"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tav <init|status|log|new|describe|commit|squash|rebase|push|advance|op log>")
		os.Exit(2)
	}

	cmd := os.Args[1]
	var err error

	switch cmd {
	case "init":
		err = tav.Init(".")
	case "status":
		err = tav.Status(".")
	case "log":
		revspec, perr := parseLogArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.LogWithRev(".", revspec)
	case "new":
		opts, perr := parseNewArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.NewWithOptions(".", opts)
	case "describe":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: tav describe <description>")
			os.Exit(2)
		}
		err = tav.Describe(".", strings.Join(os.Args[2:], " "))
	case "commit":
		message := ""
		if len(os.Args) > 2 {
			message = strings.Join(os.Args[2:], " ")
		}
		err = tav.Commit(".", message)
	case "squash":
		opts, perr := parseSquashArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.SquashWithOptions(".", opts)
	case "rebase":
		opts, perr := parseRebaseArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.RebaseWithOptions(".", opts)
	case "push":
		opts, perr := parsePushArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.Push(".", opts)
	case "advance":
		opts, perr := parseAdvanceArgs(os.Args[2:])
		if perr != nil {
			fmt.Fprintln(os.Stderr, "error:", perr)
			os.Exit(2)
		}
		err = tav.Advance(".", opts)
	case "op":
		if len(os.Args) < 3 || strings.TrimSpace(os.Args[2]) != "log" {
			fmt.Fprintln(os.Stderr, "usage: tav op log")
			os.Exit(2)
		}
		err = tav.OpLog(".")
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func parseLogArgs(args []string) (string, error) {
	if len(args) == 0 {
		return "", nil
	}
	if len(args) == 2 && args[0] == "-r" {
		if strings.TrimSpace(args[1]) == "" {
			return "", fmt.Errorf("usage: tav log [-r <change-id|A..B|..B|A..>]")
		}
		return strings.TrimSpace(args[1]), nil
	}
	return "", fmt.Errorf("usage: tav log [-r <change-id|A..B|..B|A..>]")
}

func parseRebaseArgs(args []string) (tav.RebaseOptions, error) {
	var opts tav.RebaseOptions
	for i := 0; i < len(args); i++ {
		arg := args[i]
		readValue := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			i++
			v := strings.TrimSpace(args[i])
			if v == "" {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			return v, nil
		}

		switch arg {
		case "--source", "-s":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Source = v
		case "--branch", "-b":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Branch = v
		case "--revisions", "-r":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			for _, part := range strings.Split(v, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					opts.Revisions = append(opts.Revisions, part)
				}
			}
		case "--onto", "-o":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Onto = v
		case "--insert-after", "-A":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertAfter = v
		case "--insert-before", "-B":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertBefore = v
		default:
			return opts, fmt.Errorf("unknown rebase argument: %s", arg)
		}
	}
	return opts, nil
}

func parseNewArgs(args []string) (tav.NewOptions, error) {
	var opts tav.NewOptions
	for i := 0; i < len(args); i++ {
		arg := args[i]
		readValue := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			i++
			v := strings.TrimSpace(args[i])
			if v == "" {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			return v, nil
		}
		parseRevList := func(v string) []string {
			out := make([]string, 0, 4)
			for _, part := range strings.Split(v, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					out = append(out, part)
				}
			}
			return out
		}

		switch arg {
		case "-m", "--message":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Message = v
		case "--no-edit":
			opts.NoEdit = true
		case "-A", "--insert-after", "--after":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertAfter = append(opts.InsertAfter, parseRevList(v)...)
		case "-B", "--insert-before", "--before":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertBefore = append(opts.InsertBefore, parseRevList(v)...)
		case "-o", "-r":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Parents = append(opts.Parents, parseRevList(v)...)
		default:
			if strings.HasPrefix(arg, "-") {
				return opts, fmt.Errorf("unknown new argument: %s", arg)
			}
			opts.Parents = append(opts.Parents, parseRevList(arg)...)
		}
	}
	return opts, nil
}

func parsePushArgs(args []string) (tav.PushOptions, error) {
	opts := tav.PushOptions{Remote: "origin"}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		readValue := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			i++
			v := strings.TrimSpace(args[i])
			if v == "" {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			return v, nil
		}
		parseRevList := func(v string) []string {
			out := make([]string, 0, 4)
			for _, part := range strings.Split(v, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					out = append(out, part)
				}
			}
			return out
		}

		switch arg {
		case "--remote":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Remote = v
		case "-r":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Revisions = append(opts.Revisions, parseRevList(v)...)
		case "-c":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Create = append(opts.Create, parseRevList(v)...)
		default:
			return opts, fmt.Errorf("unknown push argument: %s", arg)
		}
	}
	return opts, nil
}

func parseSquashArgs(args []string) (tav.SquashOptions, error) {
	var opts tav.SquashOptions
	for i := 0; i < len(args); i++ {
		arg := args[i]
		readValue := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			i++
			v := strings.TrimSpace(args[i])
			if v == "" {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			return v, nil
		}
		parseRevList := func(v string) []string {
			out := make([]string, 0, 4)
			for _, part := range strings.Split(v, ",") {
				part = strings.TrimSpace(part)
				if part != "" {
					out = append(out, part)
				}
			}
			return out
		}

		switch arg {
		case "-r", "--revision":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Revision = v
		case "-f", "--from":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.From = append(opts.From, parseRevList(v)...)
		case "-t", "--into", "--to":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Into = v
		case "-o", "--onto", "-d", "--destination":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Onto = append(opts.Onto, parseRevList(v)...)
		case "-A", "--insert-after", "--after":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertAfter = append(opts.InsertAfter, parseRevList(v)...)
		case "-B", "--insert-before", "--before":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.InsertBefore = append(opts.InsertBefore, parseRevList(v)...)
		case "-m", "--message":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Message = v
		case "-u", "--use-destination-message":
			opts.UseDestinationMessage = true
		case "-k", "--keep-emptied":
			opts.KeepEmptied = true
		case "--editor", "-i", "--interactive", "--tool":
			return opts, fmt.Errorf("%s is not implemented in tav squash yet", arg)
		default:
			if strings.HasPrefix(arg, "-") {
				return opts, fmt.Errorf("unknown squash argument: %s", arg)
			}
			// FILESETS are not implemented yet.
			return opts, fmt.Errorf("fileset arguments are not implemented yet: %s", arg)
		}
	}
	return opts, nil
}

func parseAdvanceArgs(args []string) (tav.AdvanceOptions, error) {
	opts := tav.AdvanceOptions{
		Remote: "origin",
		Revset: "@-",
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		readValue := func() (string, error) {
			if i+1 >= len(args) {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			i++
			v := strings.TrimSpace(args[i])
			if v == "" {
				return "", fmt.Errorf("missing value for %s", arg)
			}
			return v, nil
		}

		switch arg {
		case "--remote":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Remote = v
		case "--bookmark", "--branch", "-b":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Bookmark = v
		case "-r", "--revision":
			v, err := readValue()
			if err != nil {
				return opts, err
			}
			opts.Revset = v
		case "--force":
			opts.Force = true
		default:
			return opts, fmt.Errorf("unknown advance argument: %s", arg)
		}
	}
	return opts, nil
}
