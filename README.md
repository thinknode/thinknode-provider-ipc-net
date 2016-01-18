# thinknode-provider-ipc-net

Calculation provider IPC protocol for .NET languages

## Introduction

[NuGet](http://www.nuget.org/) is a package manager for the Microsoft .NET development platform. This project is the source code for the [Thinknode package](https://www.nuget.org/packages/Thinknode/). The package provides the `Thinknode` namespace under which is the `Provider` abstract class whose usage is described below.

## Usage

Once the `Thinknode` package has been included in your project, you must setup your class to inherit from the Thinknode.Provider class. A simplified example from the [C# Thinknode provider seed project](https://github.com/thinknode/thinknode-provider-seed-cs) is shown below.

```
using Thinknode;

namespace App
{
    class App : Provider
    {
        public static int Add(int a, int b)
        {
            return a + b;
        }

        public class void Main(string[] args)
        {
            App app = new App();
            app.Start();
        }
    }
}
```

The `Main` method of the program is responsible for creating a new App instance and calling the `Start` instance method. The provider class expects the program to be run with the environment variables THINKNODE_HOST, THINKNODE_PORT, and THINKNODE_PID set to the proper values. After connecting to the socket host (the calculation supervisor) and registering itself as a provider, it will begin to wait for a message from the supervisor.

## Packaging

### Prerequisites

- *nix OS
- `mono` installed (See http://www.mono-project.com/download/)
- nuget.exe downloaded (See http://docs.nuget.org/consume/installing-nuget)

### Step-by-step Instructions

1. Update the version in the `Thinknode/Thinknode.nuspec` file.
2. Move to the `Thinknode` directory (`cd Thinknode`).
3. Run the command `mono /nuget.exe pack Thinknode.nuspec` (assuming the nuget.exe is in the `~/` directory)

## Contributing

Please consult the [Contributing](CONTRIBUTING.md) guide.